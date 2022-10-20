use std::io::{Seek, SeekFrom};
use std::{fs::File, io::Read};

fn main() {
    let mut f = File::open("mnt/test").expect("Should have been able to open the file");

    read(&mut f, 5, 5);
    read(&mut f, 0, 5);
}

fn read(f: &mut File, pos: u64, length: usize) -> usize {
    f.seek(SeekFrom::Start(pos))
        .expect("Should be able to seek");

    let mut contents = vec![0; length];
    let byte_read = f
        .read(&mut contents)
        .expect("Should have been able to read the file");

    println!(
        "read {byte_read} bytes from pos={pos}, contents={:?}",
        String::from_utf8(contents).unwrap()
    );
    return byte_read;
}
