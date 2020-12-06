use gnuplot::{Caption, Color, DashType, Figure, LineStyle, LineWidth, PointSymbol};

pub fn plot(data: &Vec<u64>) -> Figure {
    let l1: Vec<f64> = data
        .iter()
        .skip(0)
        .step_by(3)
        .map(|&x| x as f64 / 1000.)
        .collect();
    let l2: Vec<f64> = data
        .iter()
        .skip(1)
        .step_by(3)
        .map(|&x| x as f64 / 1000.)
        .collect();
    let l3: Vec<f64> = data
        .iter()
        .skip(2)
        .step_by(3)
        .map(|&x| x as f64 / 1000.)
        .collect();

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