use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CounterType {
    Tx,   // total tx
    TxIn, // tx within rack
    Rx,   // total rx
    RxIn, // rx within rack
}

impl CounterType {
    fn iter() -> std::slice::Iter<'static, CounterType> {
        use CounterType::*;
        [Tx, TxIn, Rx, RxIn].iter()
    }
}

macro_rules! impl_index_for_counter {
    ($type:ty) => {
        impl std::ops::Index<CounterType> for [$type; 4] {
            type Output = $type;
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

        impl std::ops::IndexMut<CounterType> for [$type; 4] {
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
    }
}

impl_index_for_counter!(CounterUnitData);
impl_index_for_counter!(AvgCounterUnitData);

/// The information annotated on a virtual link.
#[derive(Clone, Serialize, Deserialize)]
pub struct CounterUnit {
    pub vnodename: String, // need to be unique across all VMs, name to sth else
    pub data: [CounterUnitData; 4],
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
        for &i in CounterType::iter() {
            self.data[i].bytes = 0;
            self.data[i].num_competitors = 0;
        }
    }

    pub fn merge(&mut self, other: &CounterUnit) {
        assert_eq!(self.vnodename, other.vnodename);
        for &i in CounterType::iter() {
            self.data[i] = self.data[i] + other.data[i];
        }
    }

    pub fn subtract(&mut self, other: &CounterUnit) {
        assert_eq!(self.vnodename, other.vnodename);
        for &i in CounterType::iter() {
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

impl std::fmt::Debug for CounterUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use CounterType::*;
        let mut s = f.debug_struct("Cunit");
        s.field("vname", &self.vnodename);
        if self.is_empty() {
            s.finish_non_exhaustive()
        } else {
            s.field("Tx", &self.data[Tx])
             .field("TxIn", &self.data[TxIn])
             .field("Rx", &self.data[Rx])
             .field("RxIn", &self.data[RxIn])
             .finish()
        }
    }
}

impl std::fmt::Debug for CounterUnitData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("C")
            .field("b", &self.bytes)
            .field("n", &self.num_competitors)
            .finish()
    }
}

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Default)]
pub struct AvgCounterUnitData {
    pub bytes: u64,
    pub num_competitors: f64,
}

#[derive(Clone)]
pub struct AvgCounterUnit {
    pub vnodename: String,
    pub data: [AvgCounterUnitData; 4],
}

impl AvgCounterUnit {
    pub fn new(vnodename: &str) -> Self {
        AvgCounterUnit {
            vnodename: vnodename.to_owned(),
            data: [AvgCounterUnitData::default(); 4],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data[0].bytes + self.data[1].bytes + self.data[2].bytes + self.data[3].bytes == 0
    }

    pub fn clear_bytes(&mut self) {
        for i in 0..4 {
            self.data[i].bytes = 0;
        }
    }

    pub fn merge_counter(&mut self, other: &CounterUnit) {
        // make sure we are merge the same vnode
        assert_eq!(self.vnodename, other.vnodename);
        for i in 0..4 {
            self.data[i].bytes += other.data[i].bytes;
            self.data[i].num_competitors =
                self.data[i].num_competitors * 0.875 + other.data[i].num_competitors as f64 * 0.125;
        }
    }
}

impl From<AvgCounterUnitData> for CounterUnitData {
    fn from(a: AvgCounterUnitData) -> Self {
        let mut c = CounterUnitData {
            bytes: a.bytes,
            num_competitors: a.num_competitors.round() as _,
        };
        if c.bytes > 0 && c.num_competitors == 0 {
            c.num_competitors = 1;
        }
        c
    }
}

impl std::fmt::Debug for AvgCounterUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use CounterType::*;
        let mut s = f.debug_struct("ACunit");
        s.field("vname", &self.vnodename);
        if self.is_empty() {
            s.finish_non_exhaustive()
        } else {
            s.field("Tx", &self.data[Tx])
             .field("TxIn", &self.data[TxIn])
             .field("Rx", &self.data[Rx])
             .field("RxIn", &self.data[RxIn])
             .finish()
        }
    }
}

impl std::fmt::Debug for AvgCounterUnitData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvgC")
            .field("b", &self.bytes)
            .field("n", &self.num_competitors)
            .finish()
    }
}

impl From<AvgCounterUnit> for CounterUnit {
    fn from(a: AvgCounterUnit) -> Self {
        CounterUnit {
            vnodename: a.vnodename,
            data: [
                a.data[0].into(),
                a.data[1].into(),
                a.data[2].into(),
                a.data[3].into(),
            ],
        }
    }
}
