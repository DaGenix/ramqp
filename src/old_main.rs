extern crate futures;
extern crate tokio_core;
// extern crate tokio_dns;

use std::env;
use std::net::SocketAddr;
use std::net::{IpAddr, ToSocketAddrs};

use futures::Future;
use futures::stream::Stream;
use tokio_core::io::{copy, Io};
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;

// use tokio_dns::tcp_connect;

fn lookup_address() -> SocketAddr {
    let addrs = ("www.google.com", 80).to_socket_addrs().expect("Failed to resolve address.");
    for addr in addrs {
        if let &SocketAddr::V4(..) = &addr {
            return addr;
        }
    }
    panic!("Failed to find V4 address.");
}

fn main() {
    // let mut lp = Core::new().unwrap();

    println!("ADDR: {}", lookup_address());


    // lp.run(conn).unwrap();
}
