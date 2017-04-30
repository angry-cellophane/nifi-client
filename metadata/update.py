import json
import xml.etree.ElementTree as ET

def update_metadata_from_properties(tree):
    root = tree.getroot()
    for processor in root.iter('processors'):
        config = processor.find('config')
        if config is None:
            raise Exception('template has no <config/>')

        comments = config.find('comments')
        if comments is None:
            raise Exception('template has no <comments/>')

        properties = config.find('properties')
        placeholders = dict(filter(lambda e: '}}' in e[1],
                                                {decouple_entry(entry) for
                                                 entry in
                                                 properties.findall('entry')}))
        metadata_el = ET.Element('metadata')
        props_el = ET.SubElement(metadata_el, 'properties')
        for key, value in placeholders.iteritems():
            prop_el = ET.SubElement(props_el, 'property')
            prop_el.set('name', key)
            prop_el.text = value

        old_comments = (comments.text + '\n' if comments.text is not None else "")
        comments.text = old_comments + ET.tostring(metadata_el)

    return tree

def update_properties_from_metadata(tree):
    root = tree.getroot()
    for processor in root.iter('processors'):
        config = processor.find('config')
        if config is None:
            raise Exception('template has no <config/>')

        comments = config.find('comments')
        if comments is None:
            raise Exception('template has no <comments/>')

        metadata = comments.find('metadata')
        if not metadata:
            continue

        props = {}
        for prop in metadata.find('properties').findall('property'):
            props[prop.get('name')] = prop.text

        print(props)

        if not props:
            continue

        properties = config.find('properties')
        entries = {decouple_entry(entry) for entry in properties.findall('entry')}
        for e in properties.findall('entry'):
            prop_name = e.find('key').text
            print(prop_name)
            if prop_name not in props:
                continue

            prop_value = e.find('value')
            if prop_value is None:
                prop_value = ET.SubElement(e, 'value')

            prop_value.text = props[prop_name]

    return tree

def decouple_entry(entry):
    key = entry.find('key').text
    value = entry.find('value')
    return (key, value.text if hasattr(value,'text') else '')
