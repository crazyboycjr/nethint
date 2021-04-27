use crate::Duration;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackgroundFlowHard {
    pub enable: bool,
    // the lambda of poisson distribution
    #[serde(default)]
    pub frequency_ns: Duration,
    // how bad is the influence, should be an other distribution
    // currently we use uniform distribution, the minimum unit is link_bw / max_slots
    // weight: Bandwidth,
    // each link will have a probability to be influenced
    #[serde(default)]
    pub probability: f64,
    // the amplitude range in [1, 9], it cut down the original bandwidth of a link by up to amplitude/10.
    // for example, if amplitude = 9, it means that up to 90Gbps (90%) can be taken from a 100Gbps link.
    #[serde(default)]
    pub amplitude: usize,
    #[serde(default = "default_average_load")]
    pub average_load: f64,
    #[serde(skip)]
    pub zipf_exp: f64,
}

pub fn default_average_load() -> f64 {
    // data center 10% average load on link
    0.1
}

pub fn search_zipf_exp(amp: usize, average_load: f64) -> f64 {
    use rand::distributions::Distribution;
    use rand::{rngs::StdRng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(1);
    let mut eval = |m| -> f64 {
        let zipf = zipf::ZipfDistribution::new(amp * 10, m).unwrap();
        let repeat = 10000;
        let mut s = 0;
        for _ in 0..repeat {
            s += zipf.sample(&mut rng);
        }
        s as f64 / repeat as f64
    };

    let mut l = 0.1;
    let mut r = 10.0;
    while l + 1e-5 < r {
        let m = (l + r) / 2.;
        if eval(m) > average_load * 100.0 {
            l = m;
        } else {
            r = m;
        }
    }
    l
}

impl Default for BackgroundFlowHard {
    fn default() -> Self {
        Self {
            enable: false,
            frequency_ns: 0,
            probability: 0.0,
            amplitude: 0,
            average_load: default_average_load(),
            zipf_exp: 0.,
        }
    }
}

#[derive(Debug, Clone, Copy, Error)]
#[error("background flow hard parse error in stage {0}")]
pub struct BackgroundFlowHardParseError(usize);

impl std::str::FromStr for BackgroundFlowHard {
    type Err = BackgroundFlowHardParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        macro_rules! bfhp_err {
            ($s:expr) => {
                BackgroundFlowHardParseError($s)
            };
        }
        if s.is_empty() {
            return Ok(Default::default());
        }
        // the format is freq:prob:amp[:avg_load]

        let tokens: Vec<&str> = s.split(':').collect();
        if tokens.len() != 3 && tokens.len() != 4 {
            return Err(bfhp_err!(1));
        }
        let frequency_ns = tokens[0].parse().map_err(|_| bfhp_err!(2))?;
        let probability = tokens[1].parse().map_err(|_| bfhp_err!(3))?;
        let amplitude = tokens[2].parse().map_err(|_| bfhp_err!(4))?;
        let average_load = if tokens.len() > 3 {
            tokens[3].parse().map_err(|_| bfhp_err!(5))?
        } else {
            default_average_load()
        };
        let zipf_exp = search_zipf_exp(amplitude, average_load);
        Ok(BackgroundFlowHard {
            enable: true,
            frequency_ns,
            probability,
            amplitude,
            average_load,
            zipf_exp,
        })
    }
}

impl ToString for BackgroundFlowHard {
    fn to_string(&self) -> String {
        if self.enable {
            format!(
                "{}:{}:{}:{}",
                self.frequency_ns, self.probability, self.amplitude, self.average_load
            )
        } else {
            String::new()
        }
    }
}
