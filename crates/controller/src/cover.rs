//! 自动封面：1146×717 白字，各行居中：投稿是封面名、开播时间两行，合集只有一行。
//! 底图默认纯黑；房间在控制台上传了背景（[`background`] 规整过的 JPEG）就原色垫在字下面。
//! 版式照 biliup 的封面（`biliup-cli/src/server/common/cover_generator.rs`）：所有行共用一个字号。

use ab_glyph::{Font, FontRef, PxScale, ScaleFont, point};
use image::{Rgb, RgbImage};

/// 思源黑体 CN Bold，SIL OFL 1.1，许可证见 `assets/fonts/OFL.txt`。
const FONT: &[u8] = include_bytes!("../assets/fonts/SourceHanSansCN-Bold.otf");
pub const WIDTH: u32 = 1146;
pub const HEIGHT: u32 = 717;
const MARGIN: f32 = 90.0;
/// 各行共用的最大字号；最宽的一行放不下就整体等比缩小，不换行。
const MAX_PX: f32 = 110.0;
/// 行距：字高的这么多倍。
const GAP_RATIO: f32 = 0.35;
/// 第二行的开播时间格式，同 biliup 封面。
pub const TIME_FORMAT: &str = "%Y-%m-%d %H点场";

/// 生成封面 JPEG：各行居中排成一列。投稿封面是封面名、开播时间两行，合集封面只有合集名一行。
/// `bg` 是 [`background`] 存下的 JPEG；空或解不开就用黑底。
pub fn render(bg: Option<&[u8]>, lines: &[&str]) -> Vec<u8> {
    jpeg(&canvas(bg, lines))
}

/// 校验控制台传来的背景：必须是 1146×717 的 JPEG（页面里已裁好、压好）。
pub fn background(jpeg: &[u8]) -> Result<(), String> {
    let img = image::load_from_memory_with_format(jpeg, image::ImageFormat::Jpeg)
        .map_err(|e| format!("背景图解不开：{e}"))?;
    match (img.width(), img.height()) {
        (WIDTH, HEIGHT) => Ok(()),
        (w, h) => Err(format!("背景图要 {WIDTH}×{HEIGHT}，传来的是 {w}×{h}")),
    }
}

fn jpeg(img: &RgbImage) -> Vec<u8> {
    let mut out = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 92)
        .encode_image(img)
        .expect("jpeg encodes into memory");
    out
}

/// 底图保留原色；没有、解不开或尺寸不对就是黑底。
fn base(bg: Option<&[u8]>) -> RgbImage {
    let bg = bg
        .and_then(|b| image::load_from_memory_with_format(b, image::ImageFormat::Jpeg).ok())
        .map(|i| i.to_rgb8())
        .filter(|i| i.dimensions() == (WIDTH, HEIGHT));
    bg.unwrap_or_else(|| RgbImage::new(WIDTH, HEIGHT))
}

fn canvas(bg: Option<&[u8]>, lines: &[&str]) -> RgbImage {
    let font = FontRef::try_from_slice(FONT).expect("embedded font parses");
    let scale = PxScale::from(font_px(&font, lines));
    let f = font.as_scaled(scale);
    let line_h = f.ascent() - f.descent();
    let gap = line_h * GAP_RATIO;
    let n = lines.len() as f32;
    let mut top = (HEIGHT as f32 - (n * line_h + (n - 1.0) * gap)) / 2.0;
    let mut img = base(bg);
    for line in lines {
        draw(&mut img, &font, line, scale, top + f.ascent());
        top += line_h + gap;
    }
    img
}

/// 共用字号：最大 [`MAX_PX`]，最宽的一行超过画布减边距就整体等比缩小。
fn font_px(font: &FontRef, lines: &[&str]) -> f32 {
    let widest = lines
        .iter()
        .map(|l| width(font, l, PxScale::from(MAX_PX)))
        .fold(0.0, f32::max);
    let max = WIDTH as f32 - 2.0 * MARGIN;
    if widest > max {
        MAX_PX * max / widest
    } else {
        MAX_PX
    }
}

fn width(font: &FontRef, text: &str, scale: PxScale) -> f32 {
    let f = font.as_scaled(scale);
    let mut prev = None;
    text.chars()
        .map(|c| {
            let id = f.glyph_id(c);
            let kern = prev.map_or(0.0, |p| f.kern(p, id));
            prev = Some(id);
            kern + f.h_advance(id)
        })
        .sum()
}

/// 画一行，按实际笔画的左右边界水平居中：全角括号等字形自带的留白不让整行偏到一边。
/// 字距仍按字体本身的宽度。字体里没有的字画成 .notdef（空心方框）。
fn draw(img: &mut RgbImage, font: &FontRef, text: &str, scale: PxScale, baseline: f32) {
    let f = font.as_scaled(scale);
    let mut x = 0.0;
    let mut prev = None;
    let mut outlines = Vec::new();
    for c in text.chars() {
        let id = f.glyph_id(c);
        x += prev.map_or(0.0, |p| f.kern(p, id));
        prev = Some(id);
        // 空格没有轮廓，只占位。
        outlines.extend(font.outline_glyph(id.with_scale_and_position(scale, point(x, baseline))));
        x += f.h_advance(id);
    }
    let left = outlines
        .iter()
        .map(|o| o.px_bounds().min.x)
        .fold(f32::MAX, f32::min);
    let right = outlines
        .iter()
        .map(|o| o.px_bounds().max.x)
        .fold(f32::MIN, f32::max);
    let shift = ((WIDTH as f32 - (right - left)) / 2.0 - left).round() as i32;
    for outline in outlines {
        let b = outline.px_bounds();
        outline.draw(|gx, gy, cov| {
            let (px, py) = (
                b.min.x as i32 + gx as i32 + shift,
                b.min.y as i32 + gy as i32,
            );
            if (0..WIDTH as i32).contains(&px) && (0..HEIGHT as i32).contains(&py) {
                // 白字按覆盖率盖在底图上。
                let a = cov.clamp(0.0, 1.0);
                let p = img.get_pixel_mut(px as u32, py as u32);
                *p = Rgb(p.0.map(|c| (c as f32 + (255.0 - c as f32) * a) as u8));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 区域内亮像素的包围盒 (左, 上, 右, 下)。
    fn ink(img: &RgbImage, y0: u32, y1: u32) -> Option<(u32, u32, u32, u32)> {
        let mut bb: Option<(u32, u32, u32, u32)> = None;
        for y in y0..y1 {
            for x in 0..img.width() {
                if img.get_pixel(x, y).0[0] > 128 {
                    bb = Some(bb.map_or((x, y, x, y), |(l, t, r, b)| {
                        (l.min(x), t.min(y), r.max(x), b.max(y))
                    }));
                }
            }
        }
        bb
    }

    fn halves(name: &str, time: &str) -> [(u32, u32, u32, u32); 2] {
        let img = canvas(None, &[name, time]);
        assert_eq!(img.dimensions(), (WIDTH, HEIGHT));
        assert_eq!(img.get_pixel(0, 0).0[0], 0, "黑底");
        let mid = HEIGHT / 2;
        [
            ink(&img, 0, mid).expect("第一行在上半"),
            ink(&img, mid, HEIGHT).expect("第二行在下半"),
        ]
    }

    #[test]
    fn two_centered_white_lines_on_black_with_one_font_size() {
        let [top, bottom] = halves("示例主播", "2026-10-23 20点场");
        // 全角括号左右各带半格留白，按字宽居中会偏左约 19px；按笔画居中左右对称。
        let [paren, _] = halves("Secret（王者组排）", "2026-10-23 20点场");
        for (l, _, r, _) in [top, bottom, paren] {
            let (left, right) = (l as i32, (WIDTH - 1 - r) as i32);
            assert!((left - right).abs() <= 3, "水平居中: {left} vs {right}");
        }
        let (above, below) = (top.1 as i32, (HEIGHT - 1 - bottom.3) as i32);
        assert!((above - below).abs() <= 40, "垂直居中: {above} vs {below}");
        // 同一字号：两行都含满高的汉字，墨迹高度相差不过几个像素。
        let (h1, h2) = (top.3 - top.1, bottom.3 - bottom.1);
        assert!(h1.abs_diff(h2) <= 6, "字号相同: {h1} vs {h2}");
        // 都放得下时用满 110px。PxScale 是整行高（ascent − descent），同 biliup 用的 imageproc，
        // 思源黑体的汉字墨迹约占其中 0.6。
        let font = FontRef::try_from_slice(FONT).unwrap();
        assert_eq!(font_px(&font, &["示例主播", "2026-10-23 20点场"]), MAX_PX);
        assert!((60..=75).contains(&h1), "110px 的汉字墨迹高: {h1}");
    }

    #[test]
    fn widest_line_shrinks_every_line_and_stays_inside_margins() {
        let long = "很长的主播名".repeat(4);
        let [top, bottom] = halves(&long, "2026-10-23 20点场");
        let (l, _, r, _) = top;
        assert!(
            l as f32 >= MARGIN - 2.0 && (WIDTH - 1 - r) as f32 >= MARGIN - 2.0,
            "{l} {r}"
        );
        // 时间行跟着一起缩小，不是只缩长的那行。
        let [_, normal] = halves("短", "2026-10-23 20点场");
        assert!(
            bottom.3 - bottom.1 < normal.3 - normal.1 - 10,
            "时间行也缩小"
        );
        assert!(font_px(&FontRef::try_from_slice(FONT).unwrap(), &[&long]) < MAX_PX);
    }

    #[test]
    fn jpeg_decodes_at_cover_size() {
        let jpeg = render(None, &["示例主播", "2026-10-23 20点场"]);
        let back = image::load_from_memory(&jpeg).unwrap();
        assert_eq!((back.width(), back.height()), (WIDTH, HEIGHT));
    }

    #[test]
    fn single_line_cover() {
        let one = render(None, &["示例主播"]);
        let back = image::load_from_memory(&one).unwrap();
        assert_eq!((back.width(), back.height()), (WIDTH, HEIGHT));
        assert_ne!(one, render(None, &["示例主播", "2026-10-23 20点场"]));
    }

    /// 纯色背景图，模拟控制台裁好上传的那张。
    fn solid(w: u32, h: u32, c: u8) -> Vec<u8> {
        jpeg(&RgbImage::from_pixel(w, h, Rgb([c, c, c])))
    }

    #[test]
    fn background_keeps_its_brightness_under_white_text() {
        let bg = solid(WIDTH, HEIGHT, 200);
        assert_eq!(background(&bg), Ok(()));
        let img = canvas(Some(&bg), &["示例主播"]);
        let corner = img.get_pixel(5, 5).0[0];
        assert!((195..=205).contains(&corner), "保留原始亮度: {corner}");
        // 字还是白的：中间那行有接近 255 的像素。
        let mid = HEIGHT / 2;
        assert!((0..WIDTH).any(|x| img.get_pixel(x, mid).0[0] > 250));
    }

    #[test]
    fn rejects_wrong_size_or_non_jpeg_and_falls_back_to_black() {
        let small = solid(100, 100, 200);
        assert!(background(&small).unwrap_err().contains("100×100"));
        assert!(background(b"not an image").is_err());
        // 存下的背景坏了也照样出封面，黑底。
        assert_eq!(
            canvas(Some(b"junk"), &["示例"]).get_pixel(0, 0).0,
            [0, 0, 0]
        );
        assert_eq!(canvas(Some(&small), &["示例"]).get_pixel(0, 0).0, [0, 0, 0]);
    }

    /// 样张：`COVER_SAMPLE=/目录 cargo test -p lubic-controller cover_sample -- --ignored`。
    /// 再设 `COVER_BG=背景.jpg` 就垫上这张背景，先过 [`background`] 的校验（见 `docs/design/cover.md`）。
    #[test]
    #[ignore]
    fn cover_sample() {
        let dir = std::path::PathBuf::from(std::env::var("COVER_SAMPLE").unwrap());
        let bg = std::env::var("COVER_BG")
            .ok()
            .map(|p| std::fs::read(p).unwrap());
        if let Some(b) = &bg {
            background(b).unwrap();
        }
        for (file, name, time) in [
            ("room-b.jpg", "房间 B（王者组排）", "2026-10-23 20点场"),
            ("room-a.jpg", "房间 A", "2026-10-23 13点场"),
        ] {
            std::fs::write(dir.join(file), render(bg.as_deref(), &[name, time])).unwrap();
        }
    }
}
