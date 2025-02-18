from openpyxl.formatting.rule import ColorScaleRule
import re


# Return type of element
def categorizeElement(element):
    element = element.replace("<", "")
    if "+" in element:
        if element[-1].isdigit():
            element = element[:-1]
        if element[-1] == "B":
            element = element[:-1]
        element = element.replace("+SyTwM", "")
        element = element.replace("+DiStM", "")
        element = element.replace("+OFTM", "")
        element = element.replace("+CiStM", "")
        element = element.replace("+SeStM", "")
        element = element.replace("+SqTwM", "")
        element = element.replace("+MiStM", "")
    if element[-1] == "V":
        element = element[:-1]
    if element[-1].isdigit():
        element = element[:-1]
    if element == "PB":
        return "Pivoting Block"
    if element[-1] == "B":
        element = element[:-1]

    element_dict = {
        "Pa": "Pair Element",
        "TrE": "Travelling Element",
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
        "GL": "Group Lift",
        "Mi": "Mixed Element",
        "PB": "Pivoting Block",
        "ChSq": "ChSq",
        "ChSl": "ChSl",
        "ChSt": "ChSt",
        "ChAJ": "ChAJ",
        "ChRS": "ChRS",
        "SyTwW": "SyTw",
        "SeStW": "SeSt",
        "DiStW": "DiSt",
        "MiStW": "MiSt",
        "CiStW": "CiSt",
        "PSt": "PSt",
        "OFTW": "OFT",
        "SqTwW": "SqTw",
        "ChTw": "ChTw",
        "PiF": "PiF",
    }

    if element in ["FiDs", "FoDs", "BiDs", "BoDs"]:
        return "Death Spiral"
    elif element in element_dict:
        return element_dict[element]
    elif element.endswith("Tw"):
        return "Twist"
    elif element in ["PSp", "PCoSp"]:
        return "Pairs Spin"
    elif element in ["StSq" or "ChSq"]:
        return element
    elif element.endswith("Li"):
        return "Lift"
    elif element.endswith("Sp"):
        return "Spin"
    elif element.endswith("Th"):
        return "Lift"
    elif element[0] in ["1", "2", "3", "4"] and element[1] in ["A", "S", "T", "L", "F"]:
        return "Jump"
    elif element.endswith("+pi") or element == "I":
        return "Intersection"
    elif element.startswith("NHE"):
        return "No Hold Element"
    elif "+kp" in element:
        if element.startswith("StSq"):
            return "StSq"
        return "Pattern dance"
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
    worksheet.conditional_formatting.add("F2:H200", color_scale_rule)
    for cell in worksheet["F"]:
        cell.number_format = "0%"
    for cell in worksheet["G"]:
        cell.number_format = "0%"
    for cell in worksheet["H"]:
        cell.number_format = "0%"


# print(categorizeElement("SyTwW4+SyTwMB"))
