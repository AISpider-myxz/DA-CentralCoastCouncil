from . import BaseItem
from scrapy import Field


class CentralCoastItem(BaseItem):
    application_id = Field()
    application_num = Field()
    description = Field()
    lodgement_date = Field()
    status = Field()
    responsible_officer = Field()
    address = Field()
    decision = Field()
    decision_date = Field()
    names = Field()
    documents = Field()

    class Meta:
        table = 'central_coast'
        unique_fields = ['application_id']
        
