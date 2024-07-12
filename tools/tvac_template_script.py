from jinja2 import Environment
from jinja2 import FileSystemLoader

my_template = Environment(loader=FileSystemLoader('.'))

indexes = [] 
for sub_channel_index in range(1,128):
    indexes.append( '{0}'.format(str(sub_channel_index).zfill(3)))


# indexes = {
# "pxie_index": "6",
# "sub_channel_index": "001"
# }

template = my_template.get_template("yamcs_config.jinja2")
result = template.render(indexes=indexes)
print(result)   