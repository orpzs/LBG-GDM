from typing import List
from vertexai.preview.generative_models import Content, Part

def tools_calls_in_content(content: Content) -> bool:
    for part in content.parts:
        if part.function_call is not None:
            return True
    return False

def get_full_content_text(content: Content) -> str:
    full_text = ""
    for part in content.parts:
        if part.text is not None:
            full_text += part.text
    return full_text

def get_full_parts_text(parts: List[Part]) -> str:
    full_text = ""
    for part in parts:
        if part.text is not None:
            full_text += part.text
    return full_text