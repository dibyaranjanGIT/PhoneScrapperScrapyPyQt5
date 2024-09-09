# myproject/exporters.py
from scrapy.exporters import CsvItemExporter

class CustomCsvItemExporter(CsvItemExporter):
    def __init__(self, *args, **kwargs):
        delimiter = kwargs.pop('delimiter', ',')
        kwargs['delimiter'] = delimiter
        super(CustomCsvItemExporter, self).__init__(*args, **kwargs)
        self.fields_to_export = ['url', 'phone_number1', 'phone_number2', 'phone_number3']
        self.first_item = True  # Flag to handle header writing on first item

    def export_item(self, item):
        phone_numbers = item.get('phone_numbers', [])
        
        # Prepare item for export
        itemdict = dict(self._get_serialized_fields(item, default_value=''))
        
        # Populate phone number fields
        for i in range(3):
            col_name = f'phone_number{i+1}'
            itemdict[col_name] = phone_numbers[i] if i < len(phone_numbers) else ''
        
        # Remove the original phone_numbers field
        if 'phone_numbers' in itemdict:
            del itemdict['phone_numbers']

        if self.first_item:
            # Write headers dynamically on the first item
            self.csv_writer.writerow(self.fields_to_export)
            self.first_item = False

        self.csv_writer.writerow([itemdict.get(key, '') for key in self.fields_to_export])
