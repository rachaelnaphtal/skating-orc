from openpyxl.formatting.rule import ColorScaleRule
import re


# ISU spin codes: level/position suffixes include ``3p4``, ``3pB``, ``1B``, ``3V1`` (reduced value).
_SPIN_CODE_RE = re.compile(
    r"^(?P<base>"
    r"FCCoSp|FCSSp|FCoSp|CCoSp|CoSp|PCoSp|"
    r"FCSp|FLSp|FUSp|FSSp|CSSp|CCSp|CLSp|CUSp|"
    r"USp|LSp|CSp|SSp|DSp|PSp"
    r")"
    r"(?:\d+p(?:\d+|B)|\d+B|\d+(?:V\d*)?)?"
    r"$",
    re.IGNORECASE,
)


def strip_element_level_suffix(element: str) -> str:
    """
    Remove trailing level / position suffix from protocol element codes for categorization.

    Examples: ``CCoSp3`` → ``CCoSp``, ``CoSp3p4`` → ``CoSp``, ``FCSp4V`` → ``FCSp``,
    ``FSSp3V1`` → ``FSSp``.
    Non-spin codes fall back to stripping a single trailing digit (``StSq4`` → ``StSq``).
    """
    element = (element or "").strip()
    if not element:
        return ""
    m = _SPIN_CODE_RE.match(element)
    if m:
        return m.group("base")
    if element[-1].isdigit():
        return element[:-1]
    return element


# Return type of element
def categorizeElement(element):
    element = strip_element_level_suffix((element or "").replace("<", "").strip())
    if not element:
        return ""
    if "+fm" in element:
        return "Moves Element"
    if "+" in element:
        if element[-1].isdigit():
            element = element[:-1]
        if element[-1].lower() == "b":
            element = element[:-1]
        element = element.replace("+SyTwM", "")
        element = element.replace("+DiStM", "")
        element = element.replace("+OFTM", "")
        element = element.replace("+OFStM", "")
        element = element.replace("+PStM", "")
        element = element.replace("+CiStM", "")
        element = element.replace("+SeStM", "")
        element = element.replace("+SqTwM", "")
        element = element.replace("+MiStM", "")
        element = element.replace("+pSTwM", "")
        element = element.replace("+SqTwM", "")
        element = element.replace("+fm", "")
        element = element.replace("+d", "")
        # element = element.replace("+SeEe", "")
    if not element:
        return ""
    if element[-1] == "V" or element[-1] == "v":
        element = element[:-1]
    if not element:
        return ""
    if element == "PB":
        return "Pivoting Block"
    if element[-1] == "B":
        element = element[:-1]
    if not element:
        return ""

    element_dict = {
        "Pa": "Pair Element",
        "TrE": "Travelling Element",
        "TE": "Twizzle Element",
        "TW": "Travelling Element",
        "TC": "Travelling Element",
        "ME": "Moves Element",
        "TwE": "Twizzle Element",
        "AL": "Artistic",
        "AC": "Artistic",
        "AW": "Artistic",
        "AB": "Artistic",
        "A": "Artistic",
        "L": "Linear/Rotating",
        "C": "Linear/Rotating",
        "B": "Linear/Rotating",
        "W": "Linear/Rotating",
        "Cr": "Creative",
        "CrL": "Creative",
        "CrI": "Creative",
        "GL": "Group Lift",
        "Co": "Mixed Element",
        "Mi": "Mixed Element",
        "PB": "Pivoting Block",
        "ChSq": "ChSq",
        "pChSq": "ChSq",
        "ChSl": "Choreo Element",
        "ChSt": "Choreo Element",
        "chst": "Choreo Element",
        "pChSt": "Choreo Element",
        "ChAJ": "Choreo Element",
        "ChRS": "Choreo Element",
        "ChHy": "Choreo Element",
        "SyTwW": "Twizzle",
        "SeStW": "Step Sequence",
        "DiStW": "Step Sequence",
        "MiStW": "Step Sequence",
        "pSTwW": "Twizzle",
        "CiStW": "Step Sequence",
        "SoPSt": "Step Sequence",
        "NtSeSt":"Step Sequence",
        "OFStL": "Step Sequence",
        'SyTwL': "Twizzle",
        'PStL':"Step Sequence",
        "PSt": "Step Sequence",
        "NtMiSt": "Step Sequence",
        "NtDiSt": "Step Sequence",
        "SqTwL": "Twizzle",
        "PStW": "Step Sequence",
        "OFTW": "Step Sequence",
        "SoOFT": "Step Sequence",
        "CCiSt":"Step Sequence",
        "CSeSt":"Step Sequence",
        "CCiSt":"Step Sequence",
        "ACiSt":"Step Sequence",
        "SpSt":"Step Sequence",
        "SqTw": "Twizzle",
        "pSoTw": "Twizzle",
        "ChTw": "Twizzle",
        "SoSqTw": "Twizzle",
        "SoTw": "Twizzle",
        "SqTw": "Twizzle",
        "STw":"Twizzle",
        "NtMiTw":"Twizzle",
        "FSTw":"Twizzle",
        "BSTw":"Twizzle",
        "SqTwW": "Twizzle",
        "PiF": "Pivot Figure",
        "DiSt": "Step Sequence",
        "NtCiSt": "Step Sequence",
        'SlSt':"Step Sequence",
        'GW1Se':"Pattern dance",
        'GW2Se':"Pattern dance",
        "SpSq":"Spiral Sequence",
        "MiStNt":"Step Sequence",
        "OFT": "Step Sequence",
        "OFSt": "Step Sequence",
        "OFStW": "Step Sequence",
        "MiSt": "Step Sequence",
        "NtMiSt1": "Step Sequence",
        "NtDiSt1": "Step Sequence",
        "SpEe": "Edge Element",
        "SeEe": "Edge Element",
        "CiSt": "Step Sequence",
        "CrEe": "Edge Element",
        "IBEe": "Edge Element",
        "SeSt": "Step Sequence",
        "1Wz": "Jump",
        "SoOFSt": "Step Sequence",
        "1MB": "Pattern dance",
        "1M": "Pattern dance",
        "1AT":"Pattern dance",
        "2AT":"Pattern dance",
    }

    if element in ["FiDs", "FoDs", "BiDs", "BoDs"]:
        return "Death Spiral"
    elif element in element_dict:
        return element_dict[element]
    elif "+kp" in element:
        if element.startswith("StSq"):
            return "Step Sequence"
        return "Pattern dance"
    elif element.endswith("Tw"):
        return "Twist"
    elif element in ["PSp", "PCoSp"]:
        return "Pairs Spin"
    elif element == "StSq":
        return "Step Sequence"
    elif element in ["ChSq"]:
        return element
    elif element.endswith("Li"):
        return "Lift"
    elif element.lower().endswith("sp"):
        return "Spin"
    elif element.endswith("Th"):
        return "Throw Jump"
    elif len(element) >= 2 and element[0] in ["1", "2", "3", "4"] and element[1].lower() in ["a", "s", "t", "l", "f", "h"]:
        return "Jump"
    elif element.endswith("+pi") or element == "I":
        return "Intersection"
    elif element.startswith("NHE"):
        return "No Hold Element"
    elif re.search(r'\dSq$', element):
        return "Pattern dance"
    elif re.search(r'\dSq\dSe$', element):
        return "Pattern dance"
    elif element.strip().endswith("Ee"):
        return "Edge Element"
    elif element.strip().startswith("A+"):
        return "Jump"
    elif "wz" in element.lower():
        return "Jump"
    elif "pchsq" in element.lower():
        return "ChSq"
    elif element.strip().startswith("SlLi4+RoLi4*"):
        return "Lift"
    elif element.startswith("StSq"):
            return "Step Sequence"
    print(f"Unable to categorize {element}")
    return element


def format_out_of_range_sheets(worksheet):
    color_scale_rule = ColorScaleRule(
        start_type="min",
        start_color="FFFFFF",  # White
        #  mid_type='percentile', mid_value=50, mid_color='7FFFD4',
        end_type="max",
        end_color="FF0000",
    )  # Red
    worksheet.conditional_formatting.add("F2:H2000", color_scale_rule)
    for cell in worksheet["F"]:
        cell.number_format = "0%"
    for cell in worksheet["G"]:
        cell.number_format = "0%"
    for cell in worksheet["H"]:
        cell.number_format = "0%"


# print(categorizeElement("SyTwW4+SyTwMB"))
