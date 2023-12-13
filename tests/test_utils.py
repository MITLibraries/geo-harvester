from harvester.utils import convert_lang_code


def test_language_code_converter_2_letter_success():
    assert convert_lang_code("en") == "eng"


def test_language_code_converter_3_letter_success():
    assert convert_lang_code("eng") == "eng"


def test_language_code_converter_returns_none():
    assert convert_lang_code("bad_lang_code") is None
