pub struct Communicator {
    my_rank: usize,
    nodes: Vec<Nodes>,
    peers: Vec<TcpStream>,
}


impl Communicator {
    pub fn new(controller_uri: &str, num_workers: usize) -> Result<Self> {
    }
}
