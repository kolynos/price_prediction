'''
Created on 09.09.2014

@author: colinos
'''
import datetime

from ebaysdk.finding import Connection as finding
from ebaysdk.trading import Connection as trading
from ebaysdk.shopping import Connection as shopping
from requests.exceptions import ConnectionError
from copy import copy,deepcopy

#import os
import os.path
import pickle
import psycopg2
#from ebaysdk.soa.finditem import Connection as FindItem


def connect_db():
    global db_connection,db_cursor
    db_connection = psycopg2.connect("dbname=ebayPredictor user=postgres password=quejodes")
    db_cursor = db_connection.cursor()

def discconnect_db():
    global db_connection,db_cursor
    db_connection.close()
    db_connection=None
    


def connect_api():
    global trading_api,finding_api,shopping_api
    trading_api=trading(config_file='ebay.yaml',siteid=77)
    finding_api=finding(config_file='ebay.yaml',siteid='EBAY-DE')
    shopping_api=shopping(config_file='ebay.yaml',siteid=77)
    




def get_categories():
    global trading_api
    try:
        response = trading_api.execute('GetCategories',{'CategorySiteID':77,'DetailLevel':'ReturnAll','CategoryParent':58058})
        print(response.dict())
    except ConnectionError as e:
        print(e)
        print(e.response.dict())

def get_item_complete_description(item):
    global trading_api
    trading_api.execute('GetItem', {'ItemID':item})
    print trading_api.response_dict()
    
def get_item_descriptions():
    global shopping_api
    a=2

def get_attribute_type_id_from_db(category,attribute_names):
    global db_connection,db_cursor
    
    attribute_id=None
    try:
        #attribute_ids=['categoryID']+['attributeName_'+str(x) for x in range(1,9)]
        query=' AND '.join(['attributeName_'+str(i)+' LIKE %s' for i in range(1,len(attribute_names)+1)])
        query=db_cursor.mogrify('Select attributeTypeID from itemAttributeIds where '+query,tuple(attribute_names))
        db_cursor.execute(query)
        attribute_id=db_cursor.fetchone()[0]
        
    except psycopg2.Error as e:
        print e
    return attribute_id
    
def add_attribute_id_to_db(category,attribute_names):
    global db_connection,db_cursor
    try:
        attribute_ids=['categoryID']+['attributeName_'+str(x) for x in range(1,9)]
        attribute_string='('+','.join(attribute_ids)+')'
        attribute_indices='('+','.join(['%s' for x in range(1,10)])+')'
        if(len(attribute_names)<8):
            attribute_names+=['' for i in range(len(attribute_names),8)]
        attribute_names=[category]+attribute_names
        print tuple(attribute_names)
        query=db_cursor.mogrify('Insert into itemAttributeIds'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_names))
        print query
        db_cursor.execute(query)
        db_connection.commit()
        #print query
        #db_cursorquery
        
        #=======================================================================
        # query=db_cursor.mogrify('SELECT * FROM itemids where itemId=%s;',(item,))
        # db_cursor.execute(query)
        # if(db_cursor.rowcount==0):
        #     query=db_cursor.mogrify('Insert into itemids(itemId) values(%s);',(item,))
        #     #print query
        #     db_cursor.execute(query)
        #     db_connection.commit()
        #     item_exists=False
        #=======================================================================
    except psycopg2.Error as e:
        print e
        
def get_category_aspects(category):
    global finding_api,reverse_order
    
    category_aspects=None
    filename='category_aspects_'+str(category)+'.pkl'
    if(os.path.isfile(filename)):
        pkl_file=open(filename, 'r')
        category_aspects=pickle.load(pkl_file)
        pkl_file.close()
    else:
        try:
            category_aspects=dict()
            response=finding_api.execute('findItemsByCategory',{'categoryId':category,'outputSelector':'AspectHistogram'})
            aspects=response.dict()['aspectHistogramContainer']['aspect']
            print(aspects)
            for aspect in aspects:
                print('---')
                print aspect['_name']
                print('---')
            
                category_aspects[aspect['_name']]=list()
                values=aspect['valueHistogram']
                #values.reverse()
                #print(aspect['valueHistogram'])
                for value in values:
                    category_aspects[aspect['_name']].append(value['_valueName'])
                    print(value['_valueName']+': '+value['count'])
                #category_aspects[aspect['_name']].reverse()
            #print(category_aspects)
            #output = open('category_aspects_'+str(category)+'.pkl', 'w')
            #pickle.dump(category_aspects,output,-1)
            #output.close()
        except ConnectionError as e:
            print(e)
            print(e.response.dict())
    keys=category_aspects.keys()
    for i in range(len(keys)):
        #print str(i)+' '+keys[i]
        values=category_aspects[keys[i]]
        #for j in range(len(values)):
        #    print(values[j])
        
    #print(category_aspects)
    return category_aspects


def get_aspect_filter(aspect_list):
    aspect_list=[{'aspectName':aspect.keys()[0],'aspectValueName':aspect[aspect.keys()[0]]} for aspect in aspect_list]
    #aspect_dict['aspectFilter']=aspect_list
    return aspect_list
    #for aspect in keys:

        

def recursive_find(category,aspect_history,category_counter,query_type='findItemsByCategory'):
    global category_aspects, category_order,ebay_calls
    
    #print aspect_history
    if(category_counter==len(category_aspects.keys())):
        aspect_filter=get_aspect_filter(aspect_history)
        valid=get_valid_aspects(category,aspect_filter)
        ebay_calls+=2
        
        print([x['aspectValueName'] for x in aspect_filter])
        print(ebay_calls)
        get_all_items(category,aspect_filter,query_type)
        
        #get_valid_aspects(category,aspect_filter)
        return
    #current_category=category_order[category_counter]
    current_name=category_aspects.keys()[category_counter]
    aspect_filter=get_aspect_filter(aspect_history)
    ebay_calls+=1
    valid_aspects=get_valid_aspects(category,aspect_filter)
    
    if(current_name in valid_aspects.keys()):
        for aspect in category_aspects[current_name]:
            new_history=copy(aspect_history)        
            if(aspect in valid_aspects[current_name]['names']):
                aspect_dict={current_name:aspect}
                new_history.append(aspect_dict)
                recursive_find(category,new_history,category_counter+1)
    else:
        new_history=copy(aspect_history)
        recursive_find(category,new_history,category_counter+1)
    
def get_valid_aspects(category,aspect_filter,query_type='findItemsByCategory'):
    global valid_aspect_categories
    
    query=dict()
    query['outputSelector']='AspectHistogram'
    query['categoryId']=category
    query['aspectFilter']=aspect_filter
    response=finding_api.execute(query_type,query)
    result=response.dict()
    valid_aspects=dict()
    if(result.has_key('aspectHistogramContainer')):
        aspects=result['aspectHistogramContainer']['aspect']
        for aspect in aspects:
            valid_aspects[aspect['_name']]={'names':list(),'counts':list()}
            values=aspect['valueHistogram']
            # if only one element is in the list, only a dict is returned. It needs to be converted into list
            if(type(values) is dict):
                values=[values]
            value_count=-1
            for value in values:
                if(value['_valueName'].find('oder mehr')==-1 or 
                   (value['_valueName'].find('oder mehr')>0 and value['count']>value_count)):
                    valid_aspects[aspect['_name']]['names'].append(value['_valueName'])
                    valid_aspects[aspect['_name']]['counts'].append(value['count'])
                    if(value['_valueName'].find('oder mehr')>0):
                        value_count=value['count']
                    #print(values[j]['_valueName'])
            
            #if(valid_aspect_categories.containaspect_category['_name'])
    return valid_aspects
    
    #result=response.dict()

def get_all_items(category,aspect_filter,query_type):
    global finding_api,ebay_calls,pickle_files
    
    try:
        query=dict()
        query['sortOrder']='StartTimeNewest'
        query['categoryId']=category
        query['aspectFilter']=aspect_filter
        response=finding_api.execute(query_type,query)
        ebay_calls+=1
        result=response.dict()
        print result['paginationOutput']
        current_page=0
        num_pages=int(result['paginationOutput']['totalPages'])
        while(current_page<num_pages):
            result=response.dict()
            result['category']=category
            result['aspectFilter']=aspect_filter
            result['page']=current_page
            output = open('items_'+str(category)+'_'+str(pickle_files)+'_'+str(current_page)+'.pkl', 'w')
            pickle.dump(result,output,-1)
            output.close()
            current_page+=1
            pickle_files+=1
            if(current_page<num_pages):
                response=finding_api.next_page()
                ebay_calls+=1
        #
        
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
        

def add_item_to_db(item,category):
    global db_connection,db_cursor
    item_exists=True
    try:
        query=db_cursor.mogrify('SELECT * FROM itemids where itemId=%s;',(item,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            query=db_cursor.mogrify('Insert into itemids(itemId,categoryID) values(%s,%s);',(item,category))
            #print query
            db_cursor.execute(query)
            #db_connection.commit()
            item_exists=False
    except psycopg2.Error as e:
        item_exists=None
        print e
    return item_exists
        #catch     
    #str="SELECT * FROM itemids WHERE itemId = %s", (0,)
    #print str
    
    #cur.execute("SELECT * FROM itemids WHERE itemId = %s", (0,))
    #all=cur.fetchall()
    #print(all)


#Item.ItemSpecifics!!!

def add_item_state(item,timestamp):
    global db_cursor
    attribute_ids=['itemID','time_timestamp','time_end',
                   'price_current','bid_count','item_status'
                   ]
    item_data_names=['itemId',['sellingStatus',['timestamp',timestamp]],['listingInfo','endTime'],['sellingStatus','currentPrice','value'],
                     #['sellingStatus',['bidCount',0]],['sellingStatus',['sellingState','None']]]
                     ['sellingStatus',['bidCount',0]],['sellingStatus','sellingState']]
    
    #print item
    attribute_string,attribute_indices,attribute_values=generate_query_strings(attribute_ids,item_data_names,item)
    query=db_cursor.mogrify('Insert into itemState'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
    #print query
    db_cursor.execute(query)
    

def generate_query_strings(query_ids,item_names,item):
    attribute_values=[]
    #print item
    for item_data in item_names:
        if(type(item_data) is str): # value is dict entry on top level of item dict
            attribute_values.append(item[item_data])
        elif(type(item_data) is int): # value is not in item dict, just insert value in db
                attribute_values.append(item_data)
        elif(type(item_data) is list): # value is a nested entry of item dict
            item_dict=item[item_data[0]]
            for element in range(1,len(item_data)-1):
                if(item_data[element] in item_dict):
                    item_dict=item_dict[item_data[element]]
                #else:
                #    print item_data
                #    print item
                #    print 'here '+item['itemId']
            last_element=item_data[-1]
            value=None
            if(type(last_element) is str): # last value of nested entry is a string
                value=item_dict[last_element]
            else:
                if(last_element[0] in item_dict): # does last dict in nested entry contain the index (or is the name in a list)?
                    if(type(item_dict) is dict): # is last entry a dict?
                        value=item_dict[last_element[0]]
                    else: # otherwise the list contains the value?
                        value=1
                else:
                    if(type(item_dict) is dict): # dict does not contain the key, use predefined value instead
                        value=last_element[1]
                    else: # list does not contain value
                        value=0
            attribute_values.append(value)
    
    attribute_string='('+','.join(query_ids)+')'
    attribute_indices='('+','.join(['%s']*len(query_ids))+')'
    return attribute_string,attribute_indices,attribute_values
    
def add_item_data(item,timestamp):
    global db_cursor
    attribute_ids=['itemID','title','condition_id','condition_name',
                   'listing_type','listing_bestoffer','listing_buyitnow','listing_buyitnow_price','listing_current_price',
                   'shipping_cost','shipping_type',
                   'payment_transfer','payment_pickup','payment_delivery','payment_paypal','payment_insured',
                   'url_item','url_gallery',
                   'time_timestamp','time_start','time_end']
    item_data_names=['itemId','title',
                     ['condition','conditionId'],['condition','conditionDisplayName'],
                     ['listingInfo','listingType'],['listingInfo','bestOfferEnabled'],['listingInfo','buyItNowAvailable'],['listingInfo','buyItNowPrice',['value',-1.0]],['sellingStatus','currentPrice',['value',-1.0]],
                     ['shippingInfo','shippingServiceCost',['value',-1.0]],['shippingInfo','shippingType'],
                     ['paymentMethod',['MoneyXferAccepted',0]],['paymentMethod',['CashOnPickup',0]],['paymentMethod',['COD',0]],['paymentMethod',['PayPal',0]],0,
                     'viewItemURL','galleryURL',
                     ['listingInfo',['timestamp',timestamp]],['listingInfo','startTime'],['listingInfo','endTime']]
    
    #print item
    attribute_string,attribute_indices,attribute_values=generate_query_strings(attribute_ids,item_data_names,item)
    #print attribute_string
    #print attribute_indices
    #print attribute_values
    query=db_cursor.mogrify('Insert into itemData'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
    db_cursor.execute(query)
    

def add_item_attributes(item,category,attributes):
    global category_aspects,db_connection,db_cursor
    item_id=int(item['itemId'])
    attribute_names=[attribute['aspectName'] for attribute in attributes]
    attribute_values=[]
    for aspect in category_aspects.keys():
        if(aspect in attribute_names):
            attribute_values.append(attributes[attribute_names.index(aspect)]['aspectValueName'])
        else:
            attribute_values.append('')
    try:
        query=db_cursor.mogrify('SELECT * FROM itemAttributes where itemId=%s;',(item_id,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            attribute_type_id=get_attribute_type_id_from_db(category,category_aspects.keys())
            attribute_ids=['itemID','attributeTypeID']+['attributeValue_'+str(x) for x in range(1,9)]
            attribute_string='('+','.join(attribute_ids)+')'
            attribute_indices='('+','.join(['%s' for x in range(10)])+')'
            if(len(attribute_names)<8):
                attribute_values+=['']* (8-len(attribute_values))
            attribute_values=[item_id,attribute_type_id]+attribute_values
            query=db_cursor.mogrify('Insert into itemAttributes'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
            #print query
            db_cursor.execute(query)
    except psycopg2.Error as e:
        print e  

def get_item_states(category):
    a=2

def load_pickle_items(category):
    global db_connection
    counter=0
    filename='items_'+str(category)+'_'+str(counter)+'_0.pkl'
    while(os.path.isfile(filename)):
    #while(counter<1):
        print counter
        pkl_file=open(filename, 'r')
        result=pickle.load(pkl_file)
        pkl_file.close()
        #print result
        for item in result['searchResult']['item']:
            item_exists=add_item_to_db(int(item['itemId']),category)
            if(not item_exists):
                add_item_attributes(item,category,result['aspectFilter'])
                add_item_data(item,result['timestamp'])
                add_item_state(item,result['timestamp'])
            #if(item_exists):
            #    print 'item already there'
            #print item['itemId']
        
        #print result
        counter+=1
        filename='items_'+str(category)+'_'+str(counter)+'_0.pkl'
    db_connection.commit()


def get_multiple_items(item_ids):
    global shopping_api
    
    try:
        query=dict()
        query['sortOrder']='StartTimeNewest'
        query['itemID']=item_ids
        query['IncludeSelector']='ItemSpecifics'
        response=shopping_api.execute('GetMultipleItems',query)
        result=response.dict()
        print result
        
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
    
   
def get_items_by_category(category,category_aspects,category_order,query_type='findItemsByCategory'):
    global finding_api
    try:
        
        keys=category_aspects.keys()
        for c in category_order:
            aspects=category_aspects[keys[c]]
            print keys[c]
            for a in range(len(aspects)):
                print aspects[a]
                response=finding_api.execute(query_type,{'outputSelector':'AspectHistogram','sortOrder':'StartTimeNewest','categoryId':category,'aspectFilter':{'aspectName':keys[c],'aspectValueName':aspects[a]}})
                aspects=response.dict()['aspectHistogramContainer']['aspect']
                print(aspects)
                result=response.dict()
                print(result['paginationOutput'])
                print(result['itemSearchURL']) 
    
        
        #=======================================================================
        # print(category_aspects.keys()[1])
        # print(category_aspects[category_aspects.keys()[1]][2])
        # 
        # response=finding_api.execute(query_type,{'sortOrder':'StartTimeNewest','categoryId':category,'aspectFilter':{'aspectName':category_aspects.keys()[1],'aspectValueName':category_aspects[category_aspects.keys()[1]][2]}})
        # 
        # result=response.dict()
        # print(result['paginationOutput'])
        # print(result['itemSearchURL'])
        # #sprint(result['searchResult'])
        # for i in range(len(result['searchResult']['item'])):
        #     print(result['searchResult']['item'][i]['title'])
        #     print(result['searchResult']['item'][i]['viewItemURL'])
        #     #print(result['searchResult']['item'][i]['productId'])
        # 
        #=======================================================================
        #finding_api.next_page()
        #response=finding_api.response
        #print(response.dict()['paginationOutput'])
        #response = finding_api.response_dict()
        #print 'here2'
        #print(finding_api.response_dict())
        #r=response.dict()
        
        #output = open('items_by_category.pkl', 'w')
        #pickle.dump(r,output,-1)
        #output.close()
        #r2=r['searchResult']
        #print(response)
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
    



    #api = FindItem(config_file='ebay.yaml')
    #records = api.find_items_by_ids([121429757727])
    
    #print(records)
    
    #api = Connection(config_file='ebay.yaml', debug=False)
    #api = Connection(appid='ColinBau-c01c-4e3d-85cf-380674b047ac')
    #response = api.execute('findItemsAdvanced', {'keywords': 'legos'})
    
    #api = shopping(config_file='ebay.yaml',siteid=77)
    #response = api.execute('GetSingleItem', {'ItemID': '121429757727'})
    api = trading(config_file='ebay.yaml',siteid=77)
    response = api.execute('GetItem', {'ItemID': '111453544170'})
    print(response.dict())

#===============================================================================
#     assert(response.reply.ack == 'Success')
#     assert(type(response.reply.timestamp) == datetime.datetime)
#     assert(type(response.reply.searchResult.item) == list)
# 
#     item = response.reply.searchResult.item[0]
#     assert(type(item.listingInfo.endTime) == datetime.datetime)
#     assert(type(response.dict()) == dict)
#     assert(type(response.dom() == _Element))
#===============================================================================

computer=58058
notebooks=175672
apple_notebooks=111422
pc_notebooks=175672
tablets=171485
handys=15032
handys_ohne_vertrag=9355
ebay_calls=0

def get_product_details(category_id):
    global shopping_api
    
    response = shopping_api.execute('FindProducts', {'CategoryID': category_id})
    print(response.dict())



if __name__ == '__main__':
    trading_api=None
    finding_api=None
    shopping_api=None
    db_connection=None
    db_cursor=None
    
    connect_db()

    connect_api()
    
    get_multiple_items('171457056707')
    exit(1)
    
    category=apple_notebooks
    category_aspects=get_category_aspects(category)
    keys=category_aspects.keys()
    if(category==apple_notebooks):
        category_aspects.pop('Betriebssystem')
        category_aspects.pop('Marke')
        keys=category_aspects.keys()
        category_aspects['Erscheinungsjahr']=['2011','2012','2013','2014','Not Specified']
        category_aspects['Produktfamilie']=['MacBook Air', 'MacBook Pro']
        category_aspects[keys[0]]=category_aspects[keys[0]][2:-1]
        category_aspects[keys[1]]=category_aspects[keys[1]][4:]
    print keys
    print category_aspects
    #add_attribute_id_to_db(apple_notebooks,keys)
    load_pickle_items(category)
    
    discconnect_db()
    exit(1)
    
    
    
    load_pickle_items(apple_notebooks)
    #add_item_to_db(0)
    discconnect_db()
    exit(0)
    
    pickle_files=0
    recursive_find(apple_notebooks,list(),0)
    #get_items_by_category(apple_notebooks,category_aspects,category_order)
    #get_items_by_category()
    #get_item_complete_description(251638689051)
    #get_product_details(apple_notebooks)
    #pkl_file = open('items_by_category.pkl', 'r')
    #data=pickle.load(pkl_file)
    #pkl_file.close()
    #print data['paginationOutput']
    #print data['searchResult']['item'][0]

    