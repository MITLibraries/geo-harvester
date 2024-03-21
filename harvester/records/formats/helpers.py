"""harvester.records.helpers"""


def gbl_resource_class_value_map() -> dict:
    """Map values to controlled gbl_resourceClass_sm values for GBL1 and Aardvark.

    https://opengeometadata.org/ogm-aardvark/#resource-class-values
    """
    return {
        "attribute": None,
        "attributeType": None,
        "collectionHardware": None,
        "collectionSession": None,
        "collections": "Collections",
        "dataset": "Datasets",
        "datasets": "Datasets",
        "dimensionGroup": None,
        "feature": None,
        "featureType": None,
        "fieldSession": None,
        "imagery": "Imagery",
        "maps": "Maps",
        "model": None,
        "nonGeographicDataset": None,
        "other": "Other",
        "property": None,
        "series": None,
        "service": None,
        "software": None,
        "tile": None,
        "web services": "Web services",
        "websites": "Websites",
    }
