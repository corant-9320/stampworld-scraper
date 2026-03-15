"""
OCR pipeline archive — not used in active build.

Kept for reference if a vision-capable model is integrated later.
The preprocessing functions here were used with EasyOCR to extract
text from stamp images, with limited success due to curved/engraved
typography. A vision LLM would be a better fit.
"""

import cv2
import numpy as np


def preprocess_for_ocr(img):
    """
    Preprocess a stamp image for better OCR accuracy.

    For stamps: upscale + contrast enhancement only.
    Binarisation is avoided as it destroys fine detail in engraved/embossed text.

    Args:
        img: BGR numpy array (original colour image)
    Returns:
        Preprocessed greyscale image optimised for EasyOCR
    """
    h, w = img.shape[:2]

    # 1. Upscale small images so text is at least ~20px tall
    min_dim = 800
    if max(h, w) < min_dim:
        scale = min_dim / max(h, w)
        img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

    # 2. Convert to LAB and apply CLAHE to L channel for contrast enhancement
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l_chan, a_chan, b_chan = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(4, 4))
    l_chan = clahe.apply(l_chan)
    lab = cv2.merge([l_chan, a_chan, b_chan])
    enhanced = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)

    # 3. Mild sharpening to crisp up letterforms
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 1))
    blurred = cv2.GaussianBlur(enhanced, (0, 0), 1.0)
    enhanced = cv2.addWeighted(enhanced, 1.5, blurred, -0.5, 0)

    # 4. Convert to greyscale — EasyOCR works well on greyscale
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_BGR2GRAY)

    return enhanced


def run_ocr_on_image(reader, img_path):
    """
    Run EasyOCR on a single image file with preprocessing, filtering out dark text.
    Returns positioned text in "word@y_norm" format for layout matching.

    reader: easyocr.Reader instance
    """
    try:
        img = cv2.imread(img_path, cv2.IMREAD_COLOR)
        if img is None:
            return ""
        img_h, img_w = img.shape[:2]

        ocr_img = preprocess_for_ocr(img)
        pp_h, pp_w = ocr_img.shape[:2]
        scale_x = img_w / pp_w
        scale_y = img_h / pp_h

        results = reader.readtext(ocr_img, detail=1)
        positioned_tokens = []
        for bbox, text, conf in results:
            pts = np.array(bbox, dtype=np.int32)
            ox_min = max(0, int(pts[:, 0].min() * scale_x))
            ox_max = min(img_w, int(pts[:, 0].max() * scale_x))
            oy_min = max(0, int(pts[:, 1].min() * scale_y))
            oy_max = min(img_h, int(pts[:, 1].max() * scale_y))
            if ox_max <= ox_min or oy_max <= oy_min:
                continue
            roi = img[oy_min:oy_max, ox_min:ox_max]
            hsv_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
            mean_sat = float(hsv_roi[:, :, 1].mean())
            mean_val = float(hsv_roi[:, :, 2].mean())
            if mean_val < 80 and mean_sat < 50:
                continue  # skip dark postmark text
            y_centre = (oy_min + oy_max) / 2.0
            y_norm = round(y_centre / img_h, 3) if img_h > 0 else 0.5
            for word in text.strip().split():
                if len(word) >= 2:
                    positioned_tokens.append(f"{word}@{y_norm}")
        return " ".join(positioned_tokens)
    except Exception:
        return ""


def run_ocr_on_bytes(reader, image_bytes):
    """
    Run EasyOCR on raw image bytes.
    Returns structured dict: {stamp_text, dark_text, entries}.
    """
    try:
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            return {"stamp_text": "", "dark_text": "", "entries": []}
        img_h, img_w = img.shape[:2]

        ocr_img = preprocess_for_ocr(img)
        pp_h, pp_w = ocr_img.shape[:2]
        scale_x = img_w / pp_w
        scale_y = img_h / pp_h

        results = reader.readtext(ocr_img, detail=1)
        stamp_parts, dark_parts, entries = [], [], []
        for bbox, text, conf in results:
            pts = np.array(bbox, dtype=np.int32)
            ox_min = max(0, int(pts[:, 0].min() * scale_x))
            ox_max = min(img_w, int(pts[:, 0].max() * scale_x))
            oy_min = max(0, int(pts[:, 1].min() * scale_y))
            oy_max = min(img_h, int(pts[:, 1].max() * scale_y))
            if ox_max <= ox_min or oy_max <= oy_min:
                continue
            roi = img[oy_min:oy_max, ox_min:ox_max]
            hsv_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
            mean_sat = float(hsv_roi[:, :, 1].mean())
            mean_val = float(hsv_roi[:, :, 2].mean())
            is_dark = mean_val < 80 and mean_sat < 50
            y_centre = (oy_min + oy_max) / 2.0
            y_norm = y_centre / img_h if img_h > 0 else 0.5
            entries.append({"text": text, "y_norm": round(y_norm, 3), "is_dark": is_dark})
            (dark_parts if is_dark else stamp_parts).append(text)
        return {
            "stamp_text": " ".join(stamp_parts).strip(),
            "dark_text": " ".join(dark_parts).strip(),
            "entries": entries,
        }
    except Exception:
        return {"stamp_text": "", "dark_text": "", "entries": []}
