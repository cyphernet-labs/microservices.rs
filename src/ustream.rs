// Channel-based non-blocking microservices without use of async
//
// SPDX-License-Identifier: Apache-2.0
//
// Written in 2022-2025 by
//     Dr. Maxim Orlovsky <orlovsky@cyphernet.org>
//
// Copyright (C) 2022-2025 Cyphernet Labs,
//                         Institute for Distributed and Cognitive Systems,
//                         Lugano, Switzerland
// All rights reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::ops::ControlFlow;

use crossbeam_channel::Sender;

use crate::UService;

#[derive(Clone, Debug)]
pub enum UStreamReq {
    Read(usize),
    Write(Vec<u8>),
    SeekStart(usize),
    SeekCurrent(isize),
    SeekEnd(usize),
}

#[derive(Debug)]
pub enum UStreamResp {
    Data(Vec<u8>),
}

pub struct UStream<T>
where T: io::Read + io::Write + ?Sized
{
    sender: Sender<UStreamResp>,
    stream: T,
}

impl<T> UStream<T> where T: io::Read + io::Write + ?Sized {}

impl<T> UService for UStream<T>
where
    UStream<T>: Send,
    T: io::Read + io::Write + ?Sized + 'static,
{
    type Msg = UStreamReq;
    type Error = io::Error;
    const NAME: &'static str = "ustream";

    fn process(&mut self, msg: Self::Msg) -> Result<ControlFlow<u8>, Self::Error> {
        match msg {
            UStreamReq::Read(len) => {
                let mut buf = vec![0; len];
                self.stream.read_exact(&mut buf)?;
                self.sender.send(UStreamResp::Data(buf))?;
            }
            UStreamReq::Write(data) => {
                self.stream.write_all(&data)?;
            }
            UStreamReq::SeekStart(pos) => self.stream.seek,
            UStreamReq::SeekCurrent(offset) => {}
            UStreamReq::SeekEnd(offset) => {}
        }
    }

    fn terminate(&mut self) { self.stream.flush().expect("unable to flush stream"); }
}
