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

fn normalize(mut l1: Vec<f64>, mut l2: Vec<f64>, mut l3: Vec<f64>) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let minv: Vec<_> = l1.iter().zip(&l2).zip(&l3).map(|((&x, &y), &z)| x.min(y).min(z)).collect();
    l1.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l2.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    l3.iter_mut().zip(&minv).for_each(|(x, &m)| *x /= m);
    (l1, l2, l3)
}

pub fn plot(data: &Vec<u64>, norm: bool) -> Figure {
    let data: Vec<f64> = data.into_iter().map(|&x| x as f64 / 1000.).collect();

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
                Caption("GeneticAlgorithm"),
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

pub fn plot_cdf(data: &Vec<u64>, norm: bool) -> Figure {
    let data: Vec<f64> = data.into_iter().map(|&x| x as f64 / 1000.).collect();

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
                Caption("GeneticAlgorithm"),
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
