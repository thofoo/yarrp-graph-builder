pub mod x_ego_betweenness {
    use petgraph::Graph;
    use petgraph::graph::{IndexType, NodeIndex};

    // Pseudocode as seen here:
    // https://link.springer.com/content/pdf/10.1007/s11036-015-0660-x.pdf, Page 475
    pub fn calculate(graph: &Graph<i32, i32>, starting_node: &NodeIndex) {
        let neighbors = graph.edges(*starting_node);
        for i in 0..1000 {
            print!("{}", i);
        }
    }

    fn dependency1(p: u32, q: u32) -> f32 {
        0.0
    }

}