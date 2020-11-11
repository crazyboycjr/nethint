use crate::inspect::JobLifetime;
use gnuplot::{Caption, Color, DashType, Figure, LineStyle, LineWidth, PointSymbol};

fn decompose(data: &[f64]) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    macro_rules! decompose_i {
        ($d:expr, $off:expr) => {{
            $d.iter()
                .skip($off)
                .step_by(3)
                .cloned()
                .collect::<Vec<f64>>()
        }};
    }
    let l1 = decompose_i!(data, 0);
    let l2 = decompose_i!(data, 1);
    let l3 = decompose_i!(data, 2);
    (l1, l2, l3)
}

fn decompose4(data: &[f64]) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    macro_rules! decompose_i {
        ($d:expr, $off:expr) => {{
            $d.iter()
                .skip($off)
                .step_by(4)
                .cloned()
                .collect::<Vec<f64>>()
        }};
    }
    let l1 = decompose_i!(data, 0);
    let l2 = decompose_i!(data, 1);
    let l3 = decompose_i!(data, 2);
    let l4 = decompose_i!(data, 3);
    (l1, l2, l3, l4)
}

fn normalize(
    mut l1: Vec<f64>,
    mut l2: Vec<f64>,
    mut l3: Vec<f64>,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    // let minv: Vec<_> = l1
    //     .iter()
    //     .zip(&l2)
    //     .zip(&l3)
    //     .map(|((&x, &y), &z)| x.min(y).min(z))
    //     .collect();
    let minv = l3.clone();
    l1.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l2.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l3.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    (l1, l2, l3)
}

fn normalize4(
    mut l1: Vec<f64>,
    mut l2: Vec<f64>,
    mut l3: Vec<f64>,
    mut l4: Vec<f64>,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    // let minv: Vec<_> = l1
    //     .iter()
    //     .zip(&l2)
    //     .zip(&l3)
    //     .zip(&l4)
    //     .map(|(((&x, &y), &z), &w)| x.min(y).min(z).min(w))
    //     .collect();
    let minv = l4.clone();
    l1.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l2.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l3.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l4.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    (l1, l2, l3, l4)
}

pub fn plot(data: &[u64], norm: bool) -> Figure {
    let data: Vec<f64> = data.iter().map(|&x| x as f64 / 1000.).collect();

    let (l1, l2, l3) = if norm {
        let (l1, l2, l3) = decompose(&data);
        normalize(l1, l2, l3)
    } else {
        decompose(&data)
    };

    let x: Vec<usize> = (1..=l1.len()).collect();
    let mut fg = Figure::new();
    fg.axes2d()
        .lines_points(
            &x,
            &l1,
            &[
                Caption("Random"),
                Color("red"),
                PointSymbol('+'),
                LineWidth(2.),
                LineStyle(DashType::DotDotDash),
            ],
        )
        .lines_points(
            &x,
            &l2,
            &[
                Caption("Plink"),
                Color("forest-green"),
                PointSymbol('x'),
                LineWidth(2.),
                LineStyle(DashType::DotDash),
            ],
        )
        .lines_points(
            &x,
            &l3,
            &[
                Caption("HierarchicalGreedy"),
                Color("blue"),
                PointSymbol('*'),
                LineWidth(2.),
                LineStyle(DashType::Dash),
            ],
        );

    fg
}

pub fn plot4(data: &[u64], norm: bool) -> Figure {
    let data: Vec<f64> = data.iter().map(|&x| x as f64 / 1000.).collect();

    let (l1, l2, l3, l4) = if norm {
        let (l1, l2, l3, l4) = decompose4(&data);
        normalize4(l1, l2, l3, l4)
    } else {
        decompose4(&data)
    };

    let x: Vec<usize> = (1..=l1.len()).collect();
    let mut fg = Figure::new();
    fg.axes2d()
        .lines_points(
            &x,
            &l1,
            &[
                Caption("Random"),
                Color("red"),
                PointSymbol('+'),
                LineWidth(2.),
                LineStyle(DashType::DotDotDash),
            ],
        )
        .lines_points(
            &x,
            &l2,
            &[
                Caption("Plink"),
                Color("forest-green"),
                PointSymbol('x'),
                LineWidth(2.),
                LineStyle(DashType::DotDash),
            ],
        )
        .lines_points(
            &x,
            &l3,
            &[
                Caption("Greedy + NetHint Level 1"),
                Color("blue"),
                PointSymbol('*'),
                LineWidth(2.),
                LineStyle(DashType::Dash),
            ],
        )
        .lines_points(
            &x,
            &l4,
            &[
                Caption("Greedy + NetHint Level 2"),
                Color("orange"),
                PointSymbol('o'),
                LineWidth(2.),
                LineStyle(DashType::Solid),
            ],
        );

    fg
}

fn to_cdf(mut data: Vec<f64>) -> (Vec<f64>, Vec<f64>) {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let &start = data.first().unwrap_or(&0.);
    let &end = data.last().unwrap_or(&0.);

    let n = data.len() as f64;
    let (mut xv, mut yv) = (Vec::new(), Vec::new());

    xv.push(start);
    yv.push(0.);

    let mut y = 0;
    for v in data {
        y += 1;
        xv.push(v);
        yv.push(y as f64 / n);
        xv.push(v);
        yv.push(y as f64 / n);
    }

    xv.push(end);
    yv.push(1.0);

    (xv, yv)
}

pub fn plot_cdf(data: &[u64], norm: bool) -> Figure {
    let data: Vec<f64> = data.iter().map(|&x| x as f64 / 1000.).collect();

    let (d1, d2, d3) = if norm {
        let (d1, d2, d3) = decompose(&data);
        normalize(d1, d2, d3)
    } else {
        decompose(&data)
    };

    let (x1, y1) = to_cdf(d1);
    let (x2, y2) = to_cdf(d2);
    let (x3, y3) = to_cdf(d3);

    let mut fg = Figure::new();
    fg.axes2d()
        .lines_points(
            &x1,
            &y1,
            &[
                Caption("Random"),
                Color("red"),
                PointSymbol('+'),
                LineWidth(2.),
                LineStyle(DashType::DotDotDash),
            ],
        )
        .lines_points(
            &x2,
            &y2,
            &[
                Caption("Plink"),
                Color("forest-green"),
                PointSymbol('x'),
                LineWidth(2.),
                LineStyle(DashType::DotDash),
            ],
        )
        .lines_points(
            &x3,
            &y3,
            &[
                Caption("HierarchicalGreedy"),
                Color("blue"),
                PointSymbol('*'),
                LineWidth(2.),
                LineStyle(DashType::Dash),
            ],
        );

    fg
}

pub fn plot_cdf4(data: &[u64], norm: bool) -> Figure {
    let data: Vec<f64> = data.iter().map(|&x| x as f64 / 1000.).collect();

    let (d1, d2, d3, d4) = if norm {
        let (d1, d2, d3, d4) = decompose4(&data);
        normalize4(d1, d2, d3, d4)
    } else {
        decompose4(&data)
    };

    let (x1, y1) = to_cdf(d1);
    let (x2, y2) = to_cdf(d2);
    let (x3, y3) = to_cdf(d3);
    let (x4, y4) = to_cdf(d4);

    let mut fg = Figure::new();
    fg.axes2d()
        .lines_points(
            &x1,
            &y1,
            &[
                Caption("Random"),
                Color("red"),
                PointSymbol('+'),
                LineWidth(2.),
                LineStyle(DashType::DotDotDash),
            ],
        )
        .lines_points(
            &x2,
            &y2,
            &[
                Caption("Plink"),
                Color("forest-green"),
                PointSymbol('x'),
                LineWidth(2.),
                LineStyle(DashType::DotDash),
            ],
        )
        .lines_points(
            &x3,
            &y3,
            &[
                Caption("Greedy + NetHint Level 1"),
                Color("blue"),
                PointSymbol('*'),
                LineWidth(2.),
                LineStyle(DashType::Dash),
            ],
        )
        .lines_points(
            &x4,
            &y4,
            &[
                Caption("Greedy + NetHint Level 2"),
                Color("orange"),
                PointSymbol('o'),
                LineWidth(2.),
                LineStyle(DashType::Solid),
            ],
        );

    fg
}

pub fn plot_segments(data: &[JobLifetime]) -> Figure {
    let mut fg = Figure::new();
    let ax = fg.axes2d();

    let mut h = 0;
    for s in data {
        h += 1;
        let x = &[s.start, s.start + s.dura];
        let y = &[h, h];
        ax.lines_points(
            x,
            y,
            &[
                Color("red"),
                PointSymbol('+'),
                LineWidth(2.),
                LineStyle(DashType::DotDotDash),
            ],
        );
    }

    fg
}
