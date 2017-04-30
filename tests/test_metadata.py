from metadata.update import update_metadata_from_properties
from metadata.update import update_properties_from_metadata
import xml.etree.ElementTree as ET
import re


def test_update_meta_from_properties():
    tree = ET.parse('./tests/resources/sample_template_with_placeholders.xml')
    updated_tree = update_metadata_from_properties(tree)

    root = updated_tree.getroot()
    for processor in root.iter('processors'):
        config = processor.find('config')
        assert config is not None

        comments = config.find('comments')
        properties = config.find('properties')
        entries = {decouple_entry(entry) for entry in properties.findall('entry')}
        placeholders = filter(lambda e: '}}' in e[1], entries)

        assert comments.text is not None
        metadata = re.findall('<metadata>.*</metadata>', comments.text)[0]
        metadata_el = ET.fromstring(metadata)

def test_update_properties_from_meta():
    tree = ET.parse('./tests/resources/sample_template.xml')
    updated_tree = update_properties_from_metadata(tree)

    root = updated_tree.getroot()
    for processor in root.iter('processors'):
        config = processor.find('config')
        assert config is not None

        comments = config.find('comments')
        metadata = comments.find('metadata')
        if not metadata:
            continue

        props = {}
        for prop in metadata.find('properties').findall('property'):
            props[prop.get('name')] = prop.text

        if not props:
            continue

        properties = config.find('properties')
        entries = {decouple_entry(entry) for entry in properties.findall('entry')}
        for (k,v) in entries:
            if k not in props:
                continue

            assert v == props[k]

def decouple_entry(entry):
    key = entry.find('key').text
    value = entry.find('value')
    return (key, value.text if hasattr(value,'text') else '')
