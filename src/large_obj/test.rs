// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::marker::PhantomData;
use std::time::Instant;

use constellation_auth::authn::BasicAuthNed;
use constellation_auth::authn::MsgAuthNTypes;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::test::TestAuthNMsgRecv;
use constellation_auth::cred::NullCred;
use constellation_common::codec::Encoder;
use constellation_common::codec::test::TestBytesCodec;
use constellation_common::codec::test::TestDecodeError;
use constellation_common::codec::test::TooShort;
use constellation_common::config::Create;
use constellation_common::error::ScopedError;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;

use crate::frags::Frags;
use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjMsgs;
use crate::large_obj::LargeObjProtoAddOutboundError;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjSender;

#[derive(Default)]
pub struct TestLargeObjProtoMsgAuthNTypes;

#[derive(Default)]
pub struct TestLargeObjProtoTypes<Msgs>
where
    Msgs: LargeObjMsgs<SHA3Algo, Vec<u8>> {
    msg: PhantomData<Msgs>
}

pub struct TestLargeObjMsgs<Msg> {
    msgs: Vec<(Option<Msg>, Option<Instant>)>
}

impl MsgAuthNTypes<Vec<u8>> for TestLargeObjProtoMsgAuthNTypes {
    type AuthNError = Infallible;
    type DecodeError = TestDecodeError;
    type Decoder = TestBytesCodec;
    type DecoderConfig = ();
    type MsgAuthN = PassthruMsgAuthN<Vec<u8>, NullCred>;
    type Prin = NullCred;
    type SessionPrin = NullCred;
    type Wrapper = Vec<u8>;
}

impl<Msgs> LargeObjProtoTypes<Vec<u8>, Vec<u8>> for TestLargeObjProtoTypes<Msgs>
where
    Msgs: LargeObjMsgs<SHA3Algo, Vec<u8>>
{
    type AuthNError = Infallible;
    type AuthNMsg = BasicAuthNed<NullCred, Vec<u8>>;
    type AuthNTypes = TestLargeObjProtoMsgAuthNTypes;
    type DecodeError = TestDecodeError;
    type Decoder = TestBytesCodec;
    type DecoderConfig = ();
    type EncodeError = TooShort;
    type Encoder = TestBytesCodec;
    type EncoderConfig = ();
    type Hash = SHA3Algo;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type MsgAuthN = PassthruMsgAuthN<Vec<u8>, NullCred>;
    type Msgs = Msgs;
    type Prin = NullCred;
    type Recv = TestAuthNMsgRecv<Vec<u8>>;
    type SessionPrin = NullCred;
    type Wrapper = Vec<u8>;
}

impl<Msg> TestLargeObjMsgs<Msg> {
    #[inline]
    pub fn new(mut msgs: Vec<(Option<Msg>, Option<Instant>)>) -> Self {
        msgs.reverse();

        TestLargeObjMsgs { msgs: msgs }
    }
}

impl<Msg> LargeObjMsgs<SHA3Algo, Msg> for TestLargeObjMsgs<Msg> {
    type AddMsgsError<Encode>
        = LargeObjProtoAddOutboundError<Encode>
    where
        Encode: Debug + Display + ScopedError;

    /// Use `sender` to add outbound large object messages.
    fn add_msgs<Enc, F>(
        &mut self,
        sender: &mut LargeObjSender<SHA3Algo, Msg, Enc, F>
    ) -> Result<Option<Instant>, Self::AddMsgsError<Enc::EncodeError>>
    where
        Enc: Clone + Create + Encoder<Msg>,
        Enc::Config: Default,
        F: Frags {
        let (msg, when) = self.msgs.pop().expect("Expected scripted message");

        if let Some(msg) = msg {
            sender.add_outbound(&msg)?;
        }

        Ok(when)
    }
}
