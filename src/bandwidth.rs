#[derive(Debug, Clone, Copy)]
enum BandwidthUnit {
    Kbps = 1000,
    Mbps = 1000_000,
    Gbps = 1000_000_000,
}

impl std::fmt::Display for BandwidthUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BandwidthUnit::*;
        match self {
            Kbps => write!(f, "Kb/s"),
            Mbps => write!(f, "Mb/s"),
            Gbps => write!(f, "Gb/s"),
        }
    }
}

pub trait BandwidthTrait {
    fn kbps(self) -> Bandwidth;
    fn mbps(self) -> Bandwidth;
    fn gbps(self) -> Bandwidth;
}

#[derive(Debug, Clone, Copy)]
pub struct Bandwidth {
    val: u64,
    unit: BandwidthUnit,
}

impl Bandwidth
{
    #[inline]
    pub fn val(&self) -> u64 {
        self.val
    }
}

impl BandwidthTrait for Bandwidth {
    fn kbps(self) -> Bandwidth {
        Bandwidth {
            val: self.val(),
            unit: BandwidthUnit::Kbps,
        }
    }
    fn mbps(self) -> Bandwidth {
        Bandwidth {
            val: self.val(),
            unit: BandwidthUnit::Mbps,
        }
    }
    fn gbps(self) -> Bandwidth {
        Bandwidth {
            val: self.val(),
            unit: BandwidthUnit::Gbps,
        }
    }
}

impl std::fmt::Display for Bandwidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.val as f64 / self.unit as u64 as f64, self.unit)
    }
}

impl std::cmp::PartialEq for Bandwidth {
    fn eq(&self, other: &Self) -> bool {
        self.val().eq(&other.val())
    }
}

impl Eq for Bandwidth {}

impl std::cmp::PartialOrd for Bandwidth {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.val().partial_cmp(&other.val())
    }
}

impl std::cmp::Ord for Bandwidth {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.val().cmp(&other.val())
    }
}

macro_rules! impl_bandwidth_div_for {
    ($($ty:ty),+ $(,)?) => (
        $(impl std::ops::Div<$ty> for Bandwidth {
            type Output = Self;
            fn div(self, rhs: $ty) -> Self::Output {
                Bandwidth {
                    val: ((self.val as f64) / rhs as f64) as u64,
                    unit: self.unit,
                }
            }
        }
        impl std::ops::Mul<$ty> for Bandwidth {
            type Output = Self;
            fn mul(self, rhs: $ty) -> Self::Output {
                Bandwidth {
                    val: ((self.val as f64) * rhs as f64) as u64,
                    unit: self.unit,
                }
            }
        })+
    )
}

impl_bandwidth_div_for!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

impl std::ops::Sub for Bandwidth {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Bandwidth {
            val: self.val - rhs.val,
            unit: self.unit,
        }
    }
}

impl std::ops::Add for Bandwidth {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Bandwidth {
            val: self.val + rhs.val,
            unit: self.unit,
        }
    }
}

macro_rules! impl_bandwidth_trait_for {
    ($($ty:ty),+ $(,)?) => (
        $(impl BandwidthTrait for $ty
        {
            fn kbps(self) -> Bandwidth {
                let unit = BandwidthUnit::Kbps;
                Bandwidth {
                    val: (self as f64 * unit as u64 as f64) as u64,
                    unit,
                }
            }
            fn mbps(self) -> Bandwidth {
                let unit = BandwidthUnit::Mbps;
                Bandwidth {
                    val: (self as f64 * unit as u64 as f64) as u64,
                    unit,
                }
            }
            fn gbps(self) -> Bandwidth {
                let unit = BandwidthUnit::Gbps;
                Bandwidth {
                    val: (self as f64 * unit as u64 as f64) as u64,
                    unit,
                }
            }
        })+
    )
}

impl_bandwidth_trait_for!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_it() {
        let a: Bandwidth = 1000.kbps();
        let b: Bandwidth = (10.1555).mbps();
        let c: Bandwidth = (9.5).gbps();
        println!("{:?}", a);
        println!("{}", b.val());
        println!("{}", c.kbps());
        println!("{}", (c - b).mbps());
        println!("{}", (c + b).mbps() / 1000);
        println!("{}", (c - b).gbps() * 1000 + a * 1000);
        let d: Bandwidth = c / 1000;
        println!("{}", d.mbps());
        println!("{}", ((c / 1000) as Bandwidth).mbps());
    }
}