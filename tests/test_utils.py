from harvester.utils import convert_lang_code, dedupe_list_of_values


def test_language_code_converter_2_letter_success():
    assert convert_lang_code("en") == "eng"


def test_language_code_converter_3_letter_success():
    assert convert_lang_code("eng") == "eng"


def test_language_code_converter_returns_none():
    assert convert_lang_code("bad_lang_code") is None


def test_dedupe_list_of_strings_single_value_list():
    assert dedupe_list_of_values([["cat"]]) == ["cat"]


def test_dedupe_list_of_strings_titlecase():
    # TitleCase
    assert dedupe_list_of_values(["HORSE", "Horse", "horse"]) == ["Horse"]
    # UPPERCASE
    assert dedupe_list_of_values(["HORSE", "horse"]) == ["HORSE"]
    # lowercase
    assert dedupe_list_of_values(["horse", "horse"]) == ["horse"]
    # order doesn't matter
    assert dedupe_list_of_values(["horse", "HORSE"]) == ["HORSE"]
    # empty list untouched
    assert dedupe_list_of_values([]) == []
    # integers deduped
    assert dedupe_list_of_values([1, 2, 2, 3]) == [1, 2, 3]
    # None is preserved as well
    assert dedupe_list_of_values(["cat", None, "cat", None]) == ["cat", None]
