from lxml import etree

def format_xml(input_file, output_file):
    try:
        tree = etree.parse(input_file)
        with open(output_file, 'wb') as outfile:
            tree.write(outfile, pretty_print=True, xml_declaration=True, encoding='utf-8')
    except Exception as e:
        print(f"Error formatting XML: {e}")

input_xml = "sitemap.xml"
output_xml = "sitemap-formatted.xml"

format_xml(input_xml, output_xml)

