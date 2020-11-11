use anyhow::Result;
use nethint::Timestamp;
use std::io::BufRead;

pub struct JobTrace {
    pub nracks: usize,
    pub count: usize,
    pub records: Vec<Record>,
}

// example: 3 13122 2 66 138 1 38:4.0
#[derive(Debug, Clone)]
pub struct Record {
    pub id: usize,
    pub ts: Timestamp,
    pub num_map: usize,
    pub mappers: Vec<usize>,
    pub num_reduce: usize,
    pub reducers: Vec<(usize, f64)>,
}

#[derive(Debug, Clone, Copy)]
pub struct ParseRecordError;

impl std::fmt::Display for ParseRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

macro_rules! parse_next {
    ($tokens:expr, $ret:ty) => {
        $tokens
            .next()
            .and_then(|f| f.parse::<$ret>().ok())
            .ok_or(ParseRecordError)?
    };
}

impl std::str::FromStr for Record {
    type Err = ParseRecordError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut tokens = s.trim().split(' ');
        let id = parse_next!(tokens, usize);
        let ts = parse_next!(tokens, Timestamp);
        let num_map = parse_next!(tokens, usize);
        let mappers: Vec<usize> = tokens
            .by_ref()
            .take(num_map)
            .map(|x| x.parse::<usize>().ok())
            .collect::<Option<Vec<_>>>()
            .ok_or(ParseRecordError)?;
        let num_reduce = parse_next!(tokens, usize);
        let reducers: Vec<(usize, f64)> = tokens
            .take(num_reduce)
            .map(|x| {
                x.split_once(":")
                    .and_then(|(a, b)| a.parse::<usize>().ok().zip(b.parse::<f64>().ok()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(ParseRecordError)?;

        assert_eq!(num_map, mappers.len());
        assert_eq!(num_reduce, reducers.len());

        Ok(Record {
            id,
            ts,
            num_map,
            mappers,
            num_reduce,
            reducers,
        })
    }
}

impl JobTrace {
    pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let f = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(f);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let v: Vec<usize> = line
            .trim()
            .split(' ')
            .map(|x| x.parse().ok())
            .collect::<Option<_>>()
            .unwrap();
        assert_eq!(v.len(), 2);

        let nracks = v[0];
        let count = v[1];
        let mut records = Vec::new();
        for _i in 0..count {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if line.starts_with('#') {
                continue;
            }
            let r: Record = line
                .parse()
                .unwrap_or_else(|e| panic!("pare line failed: {}, line: {}", e, line));
            records.push(r);
        }

        Ok(JobTrace {
            nracks,
            count,
            records,
        })
    }
}
