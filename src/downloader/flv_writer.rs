use crate::downloader::flv_parser::{
    AACPacketType, AVCPacketType, CodecId, FrameType, ScriptData, SoundFormat, SoundRate,
    SoundSize, SoundType, TagHeader,
};

use crate::downloader::util::{LifecycleFile, SegmentCloseReason, SegmentWriter};
use byteorder::{BigEndian, WriteBytesExt};
use serde::Serialize;
use std::io::Write;

/// FLV 文件头（音视频均有）+ 首个 PreviousTagSize(0)。
const FLV_PRELUDE: [u8; 13] = [b'F', b'L', b'V', 0x01, 0x05, 0, 0, 0, 0x09, 0, 0, 0, 0];

pub type FlvFile<'a> = SegmentWriter<'a>;

impl<'a> SegmentWriter<'a> {
    pub fn flv(file: LifecycleFile<'a>) -> std::io::Result<Self> {
        SegmentWriter::new(file, &FLV_PRELUDE, SegmentCloseReason::Unknown)
    }

    pub fn write_tag(
        &mut self,
        tag_header: &TagHeader,
        body: &[u8],
        previous_tag_size: &[u8],
    ) -> std::io::Result<()> {
        write_tag_header(&mut self.buf_writer, tag_header)?;
        // write 允许短写，会静默丢字节破坏 FLV 结构，必须 write_all。
        self.buf_writer.write_all(body)?;
        self.buf_writer.write_all(previous_tag_size)
    }
}

/// 11 字节 tag 头：类型、长度、时间戳低 24 位 + 扩展高 8 位、stream id。
fn write_tag_header(writer: &mut impl Write, tag_header: &TagHeader) -> std::io::Result<()> {
    writer.write_u8(tag_header.tag_type as u8)?;
    writer.write_u24::<BigEndian>(tag_header.data_size)?;
    writer.write_u24::<BigEndian>(tag_header.timestamp & 0xffffff)?;
    writer.write_u8((tag_header.timestamp >> 24) as u8)?;
    writer.write_u24::<BigEndian>(tag_header.stream_id)
}

#[derive(Debug, PartialEq, Serialize)]
pub struct FlvTag<'a> {
    pub header: TagHeader,
    pub data: TagDataHeader<'a>,
}

#[derive(Debug, PartialEq, Serialize)]
pub enum TagDataHeader<'a> {
    Audio {
        sound_format: SoundFormat,
        sound_rate: SoundRate,
        sound_size: SoundSize,
        sound_type: SoundType,
        packet_type: Option<AACPacketType>,
    },
    Video {
        frame_type: FrameType,
        codec_id: CodecId,
        packet_type: Option<AVCPacketType>,
        composition_time: Option<i32>,
    },
    Script(ScriptData<'a>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downloader::flv_parser::TagType;

    /// 每次最多接受 1 字节的 Writer：只有 write_all 才能写完整。
    struct ShortWriter(Vec<u8>);

    impl Write for ShortWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let take = buf.len().min(1);
            self.0.extend_from_slice(&buf[..take]);
            Ok(take)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn tag_header_is_fully_written_with_extended_timestamp() {
        let mut writer = ShortWriter(Vec::new());
        let header = TagHeader {
            tag_type: TagType::Video,
            data_size: 0x000102,
            timestamp: 0x0a0b0c0d,
            stream_id: 0,
        };
        write_tag_header(&mut writer, &header).unwrap();
        assert_eq!(
            writer.0,
            [9, 0x00, 0x01, 0x02, 0x0b, 0x0c, 0x0d, 0x0a, 0, 0, 0]
        );
    }
}
