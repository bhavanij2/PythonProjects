import random
import json
import datetime
from shared.login import get_user_id, get_user_doc_number


def build_preapproval_req(products, is_single):
    request = dict()
    request['loginUser'] = get_user_id()
    request['loginUserCustomerDocNumber'] = get_user_doc_number()
    request['submissionDate'] = datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
    request['items'] = build_items(get_products(products, is_single))
    return json.dumps(request)


def build_items(product_list):
    items = []
    for p in product_list:
        item = dict()
        item['id'] = p['id']
        item['customerHeadOfficeDocNumber'] = p['customerHeadOfficeDocNumber']
        item['customerHeadOfficeName'] = p['customerHeadOfficeName']
        item['district'] = p['district']
        item['harvest'] = p['harvest']
        item['brand'] = p['brand']
        item['company'] = p['company']
        item['crop'] = p['crop']
        item['product'] = p['product']
        item['region'] = p['region']
        item['technology'] = p['technology']
        item['preApprovedAmount'] = random.randint(0, 100000)
        items.append(item)
    return items


# returns a list containing a single or multiple products to submit for pre-approval
def get_products(prod_list, is_single):
    if is_single:
        return [random.choice(prod_list)]
    return prod_list



