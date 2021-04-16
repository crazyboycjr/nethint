use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CounterType {
    Tx,   // total tx
    TxIn, // tx within rack
    Rx,   // total rx
    RxIn, // rx within rack
}

impl std::ops::Index<CounterType> for [CounterUnitData; 4] {
    type Output = CounterUnitData;
    fn index(&self, index: CounterType) -> &Self::Output {
        use CounterType::*;
        match index {
            Tx => self.get(0).unwrap(),
            TxIn => self.get(1).unwrap(),
            Rx => self.get(2).unwrap(),
            RxIn => self.get(3).unwrap(),
        }
    }
}

impl std::ops::IndexMut<CounterType> for [CounterUnitData; 4] {
    fn index_mut(&mut self, index: CounterType) -> &mut Self::Output {
        use CounterType::*;
        match index {
            Tx => self.get_mut(0).unwrap(),
            TxIn => self.get_mut(1).unwrap(),
            Rx => self.get_mut(2).unwrap(),
            RxIn => self.get_mut(3).unwrap(),
        }
    }
}

/// The information annotated on a virtual link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterUnit {
    pub vnodename: String, // need to be unique across all VMs, name to sth else
    pub data: [CounterUnitData; 4], // 0 for tx, 1 for rx
}

impl CounterUnit {
    pub fn new(vnodename: &str) -> Self {
        CounterUnit {
            vnodename: vnodename.to_owned(),
            data: [CounterUnitData::default(); 4],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data[0].bytes + self.data[1].bytes + self.data[2].bytes + self.data[3].bytes == 0
    }

    pub fn clear(&mut self) {
        for i in 0..4 {
            self.data[i].bytes = 0;
            self.data[i].num_competitors = 0;
        }
    }

    pub fn merge(&mut self, other: &CounterUnit) {
        assert_eq!(self.vnodename, other.vnodename);
        for i in 0..4 {
            self.data[i] = self.data[i] + other.data[i];
        }
    }

    pub fn subtract(&mut self, other: &CounterUnit) {
        assert_eq!(self.vnodename, other.vnodename);
        for i in 0..4 {
            assert!(
                self.data[i].bytes >= other.data[i].bytes,
                "{:?} vs {:?}",
                self.data,
                other.data
            );
            self.data[i] = self.data[i] - other.data[i];
        }
    }

    pub fn add_flow(&mut self, counter_type: CounterType, delta: u64) {
        if delta > 0 {
            self.data[counter_type].bytes += delta;
            self.data[counter_type].num_competitors += 1;
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CounterUnitData {
    pub bytes: u64,           // delta in bytes in the last time slot
    pub num_competitors: u32, // it can be num_flows, num_vms, or num_tenants, depending on the fairness model
}

impl std::ops::Add for CounterUnitData {
    type Output = CounterUnitData;
    fn add(self, rhs: CounterUnitData) -> Self::Output {
        CounterUnitData {
            bytes: self.bytes + rhs.bytes,
            num_competitors: self.num_competitors + rhs.num_competitors,
        }
    }
}

impl std::ops::Sub for CounterUnitData {
    type Output = CounterUnitData;
    fn sub(self, rhs: CounterUnitData) -> Self::Output {
        CounterUnitData {
            bytes: self.bytes - rhs.bytes,
            num_competitors: self.num_competitors - rhs.num_competitors,
        }
    }
}

impl std::ops::AddAssign for CounterUnitData {
    fn add_assign(&mut self, rhs: CounterUnitData) {
        self.bytes += rhs.bytes;
        self.num_competitors += rhs.num_competitors;
    }
}

impl std::ops::SubAssign for CounterUnitData {
    fn sub_assign(&mut self, rhs: CounterUnitData) {
        self.bytes -= rhs.bytes;
        self.num_competitors -= rhs.num_competitors;
    }
}
