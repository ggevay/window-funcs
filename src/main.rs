mod prefix_sum2;

extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Consolidate, Join};
use crate::prefix_sum2::PrefixSum2;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        worker.dataflow(|scope| {

            let xs = input.to_collection(scope);

            // xs
            //     .map(|x| 2*x)
            //     .inspect(|x| println!("{:?}", x));

            //let ps = xs.prefix_sum2(0, |_k, x, y| (x+y));
            //let ps = xs.prefix_sum2(0, |_k, _x, y| (*y));
            let ps = xs.prefix_sum2(0, |_k, x, y| if *y == 0 {*x} else {*y});
            //let ps = xs.prefix_sum2("z".to_owned(), |_k, x, y| (x.clone() + "+" + y));

            let res = ps.join(&xs);

            res
                .consolidate()
                .inspect(|x|
                    println!("{:?}", x)
                    //()
                );
        });

        input.advance_to(0);

        input.insert(((3usize, ()), 5));
        input.insert(((6usize, ()), 2));
        input.insert(((8usize, ()), 3));
        input.insert(((9usize, ()), 7));
        //input.insert(((9usize, ()), 6)); // Duplicate index
        input.insert(((20usize, ()), 4));

        // for i in 0usize .. 1000000 {
        //     input.insert(((i, ()), 1));
        // }
        // input.advance_to(1);
        // input.insert(((12usize, ()), 100));

        // input.insert(((3usize, ()), "a".to_owned()));
        // input.insert(((6usize, ()), "b".to_owned()));
        // input.insert(((8usize, ()), "c".to_owned()));
        // input.insert(((9usize, ()), "d".to_owned()));
        // input.insert(((20usize, ()), "e".to_owned()));

    }).expect("Computation terminated abnormally");

}
