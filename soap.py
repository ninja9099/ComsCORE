from requests import Session
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from zeep import Client
from zeep.transports import Transport

import logging.config
import pprint
pp = pprint.PrettyPrinter(depth=6)
#globals for holding the different params values
param_list = []
param_values = {}
# logging.config.dictConfig({
#     'version': 1,
#     'formatters': {
#         'verbose': {
#             'format': '%(name)s: %(message)s'
#         }
#     },
#     'handlers': {
#         'console': {
#             'level': 'DEBUG',
#             'class': 'logging.StreamHandler',
#             'formatter': 'verbose',
#         },
#     },
#     'loggers': {
#         'zeep.transports': {
#             'level': 'DEBUG',
#             'propagate': True,
#             'handlers': ['console'],
#         },
#     }
# })


session = Session()
session.auth = HTTPBasicAuth('cmt_mleon1', '2ea6b684')
client = Client('https://api.comscore.com/DemographicProfile.asmx?wsdl',
    transport=Transport(session=session))

def getParamsList(client):
    # param_list = []
    global param_list
    response = client.service.DiscoverParameters()
    
    for param  in response['Parameter']:
        print "parameter is  ==> ", param['Id']
        param_list.append(param['Id'])
    return param_list

def GetParamValues(client, parameterId, deps=[]):
    return client.service.DiscoverParameterValues(parameterId=parameterId)


x = getParamsList(client)
for prm in param_list:
    param_values_response = GetParamValues(client, prm)
    value_list = [val for val in param_values_response['EnumValue']]
    param_values.update({prm: value_list})


pp.pprint(param_values)
import pdb; pdb.set_trace()