'''
Created on 09.09.2014

@author: colinos
'''
import datetime
import time
from ebaysdk.finding import Connection as finding
from ebaysdk.trading import Connection as trading
from ebaysdk.shopping import Connection as shopping
from ebaysdk.exception import ConnectionError as EbayConnectionError
from requests.exceptions import ConnectionError,Timeout
from copy import copy,deepcopy
from itertools import islice
#import psycopg2 as pg
import pandas.io.sql as psql
import numpy as np
import pandas as pd

#import os
import os.path
import pickle
import psycopg2
import sys
#from ebaysdk.soa.finditem import Connection as FindItem


def connect_db():
    global db_connection,db_cursor
    db_connection = psycopg2.connect("dbname=ebayPredictor user=postgres password=quejodes")
    db_cursor = db_connection.cursor()

def disconnect_db():
    global db_connection,db_cursor
    db_connection.close()
    db_connection=None
    


def connect_api():
    global trading_api,finding_api,shopping_api
    trading_api=trading(config_file='ebay.yaml',siteid=77,token='AgAAAA**AQAAAA**aAAAAA**gNoiVA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wJkYWoDpmGqA2dj6x9nY+seQ**PXkCAA**AAMAAA**hzDKea9fTGWVfQz6a+ULxm8p20w2dRegRC1r3fy5UQjAbb/gQc7m76reGs4JHLA8/Dq6hdxVx6kNmP/78rG9ii4NXu3UIjyFPDA2G4Yg+BMZ7BfDdCVgBPtigPhrrf6GJxTQF/yP0nWbFW0hDN6ZwFURMEcFXazayEUbqNDD9V0zj35Z1X7WE3xV8tnph37SDZDhplGC1ha3MjcDRZqZURhz74TxmH1nugJ9pSsjY7GsDvmVgFk0BP/AFgA8jOXiA6avHSAzWstY9AK/Pxmcj5fO3Df+w5WYrjnNMPZcwPMyFeb+oAxABG8KkYmKbWmh/D57JVHn0G6J2tEysCQMUXUZa8VxbP9noKjmIADhmYh6pjx7PzAtjB0feAdFOb9BGmVeczGgYJrMaBhujrwPB9beaC6d1EuUnw4dON/RJqHWRq9ecQkAMsVB5+eki+/HS0LA4ZVA01FaMW6QlgNRefwj5fAJbv20mggfYx5dA+e8STAmk05ThzUktiaRzHxZEtIiVsf3xqZkSFfCZVpFskdWsBb+ifLYpjEAf+KG+56zijYC50HOP+oTcVqfUcQ9EaBZyiS6+VrYR2KVkGiVGNWyOdY56Y7Q7yJfhMxMi2YAxEFyhPZt6qVg02Ka6ubV0Mcm6Xn1jq02XUzkq3+Q0QMYV2D37MGYxaq0qVCfvp3l6GuY+hZ1S7z9BPu2uXDC8h0t+JurlSveX+/2cj+2loCX+nneCgikupnwrmZCFbqV4jmX36qrIDbE7KU2d7TE')
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
    
    query={}
    query['DetailLevel']='ItemReturnDescription'
    query['ItemID']=item
    trading_api.execute('GetItem', query)
    print trading_api.response_dict()
    

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
        attribute_ids=['categoryID']+['attributeName_'+str(x) for x in range(1,17)]
        attribute_string='('+','.join(attribute_ids)+')'
        attribute_indices='('+','.join(['%s' for x in range(1,18)])+')'
        if(len(attribute_names)<16):
            attribute_names+=['' for i in range(len(attribute_names),16)]
        attribute_names=[category]+attribute_names
        query=db_cursor.mogrify('Insert into itemAttributeIds'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_names))
        db_cursor.execute(query)
        db_connection.commit()
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


def get_aspect_filter_old(aspect_list):
    aspect_list=[{'aspectName':aspect.keys()[0],'aspectValueName':aspect[aspect.keys()[0]]} for aspect in aspect_list]
    #aspect_dict['aspectFilter']=aspect_list
    return aspect_list
    #for aspect in keys:

def get_aspect_filter(aspect_list):
    aspect_list=[{'aspectName':aspect,'aspectValueName':aspect_list[aspect]} for aspect in aspect_list]
    #print aspect_list
    return aspect_list
        

def recursive_find(category,aspect_history,category_counter,query_type='findItemsByCategory'):
    global category_aspect_dict, category_order,ebay_calls
    
    #print aspect_history
    if(category_counter==len(category_aspect_dict.keys())):
        aspect_filter=get_aspect_filter(aspect_history)
        valid=get_valid_aspects(category,aspect_filter)
        ebay_calls+=2
        
        print([x['aspectValueName'] for x in aspect_filter])
        print(ebay_calls)
        get_all_items(category,aspect_filter,query_type)
        
        #get_valid_aspects(category,aspect_filter)
        return
    #current_category=category_order[category_counter]
    current_name=category_aspect_dict.keys()[category_counter]
    aspect_filter=get_aspect_filter(aspect_history)
    ebay_calls+=1
    valid_aspects=get_valid_aspects(category,aspect_filter)
    
    if(current_name in valid_aspects.keys()):
        for aspect in category_aspect_dict[current_name]:
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

def get_all_items_old(category,aspect_filter,query_type):
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
        

def get_all_items(category,aspect_filter,query_type='findItemsByCategory',stop_if_known=False):
    global finding_api,pickle_files,db_cursor,api_error,log_file
    try:
        api_error=False
        result_data=[]
        query=dict()
        query['sortOrder']='StartTimeNewest'    
        query['categoryId']=category
        if(len(aspect_filter)>0):
            query['aspectFilter']=aspect_filter
        response=finding_api.execute(query_type,query)
        result=response.dict()
        current_page=int(result['paginationOutput']['pageNumber'])
        num_pages=int(result['paginationOutput']['totalPages'])
        f=open(log_file,'a')
        f.write('get_all_items: pages='+str(result['paginationOutput']['totalPages']))
        f.write('\n')
        f.close()
        #while(current_page<=1):
        object_known=False
        while(current_page<=num_pages and not object_known):
            f=open(log_file,'a')
            f.write(str(result['paginationOutput']))
            f.write('\n')
            f.close()
            for item in result['searchResult']['item']:
                if(stop_if_known):
                    query=db_cursor.mogrify('SELECT * FROM itemData where itemId=%s;',(int(item['itemId']),))
                    db_cursor.execute(query)
                    if(db_cursor.rowcount>0):
                        f=open(log_file,'a')
                        f.write('found known object\n')
                        f.close()
                        object_known=True
                        break
                result_data.append(item['itemId'])
            if(current_page<num_pages):
                finding_api.next_page()
                response=finding_api.response
                result=response.dict()
                num_pages=min(num_pages,int(result['paginationOutput']['totalPages']))
                current_page=int(result['paginationOutput']['pageNumber'])
            else:
                current_page+=1    
    except (ConnectionError,Timeout) as e:
        print(e)
        api_error=True
        result_data=None
    return result_data
 

def get_all_items_from_db(category):
    global db_cursor
    
    query=db_cursor.mogrify('SELECT itemId FROM itemids where categoryId=%s;',(category,))
    #items=
    db_cursor.execute(query)
    items=db_cursor.fetchall()
    items=[i[0] for i in items]
    return items
    

def get_active_items_from_db(category):
    db_cursor
    query=db_cursor.mogrify('SELECT itemId FROM itemids where categoryId=%s;',(category,))
    db_cursor.execute(query)
    items=db_cursor.fetchall()
    active_items=[]
    missing_states=0
    #items=[i[0] for i in items]
    for item in items:
        query=db_cursor.mogrify('SELECT item_status,time_timestamp FROM itemstate where itemId=%s order by time_timestamp desc limit 1;',(item[0],))
        db_cursor.execute(query)
        itemstate=db_cursor.fetchone()
        if(db_cursor.rowcount==0):
            missing_states+=1
            #print 'No state data for item '+str(item[0])
        else:
            if(itemstate[0] == 'Active'):
                active_items.append((item[0],itemstate[1]))
    f=open(log_file,'a')
    f.write('nr of missing states: '+str(missing_states))
    f.write('\n')
    f.close()
    return active_items

def add_items(items,category):
    global db_connection,db_cursor,log_file
    new_data=[]
    try:
        for item in items:
            query=db_cursor.mogrify('SELECT * FROM itemids where itemId=%s;',(item,))
            db_cursor.execute(query)
            if(db_cursor.rowcount==0):
                query=db_cursor.mogrify('Insert into itemids(itemId,categoryID) values(%s,%s);',(item,category))
                db_cursor.execute(query)
                #db_connection.commit()
                new_data.append(item)
    except psycopg2.Error as e:
        f=open(log_file,'a')
        f.write(str(e))
        f.write('\n')
        f.close()
    db_connection.commit()
    return new_data
        #catch     
    #str="SELECT * FROM itemids WHERE itemId = %s", (0,)
    #print str
    
    #cur.execute("SELECT * FROM itemids WHERE itemId = %s", (0,))
    #all=cur.fetchall()
    #print(all)


#Item.ItemSpecifics!!!

def add_state(items,timestamp):
    global db_cursor,db_connection
    attribute_ids=['itemID','time_timestamp','time_end',
                   'price_current','hit_count', 'bid_count','item_status','seller_rating','seller_score','seller_feedback','seller_top'
                   ]
    item_data_names=['ItemID',{'name' :'dummy','NA':timestamp},'EndTime',['CurrentPrice',['value',-1.0]],
                     #['sellingStatus',['bidCount',0]],['sellingStatus',['sellingState','None']]]
                     'HitCount','BidCount','ListingStatus',
                     ['Seller','FeedbackRatingStar'],['Seller',['FeedbackScore',-1]],['Seller','PositiveFeedbackPercent'],['Seller',['TopRatedSeller','false']]]
    
    
    for item in items:
        attribute_string,attribute_indices,attribute_values=generate_query_strings(attribute_ids,item_data_names,item)
        query=db_cursor.mogrify('Insert into itemState'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
        db_cursor.execute(query)
    db_connection.commit()
    

def generate_query_strings(query_ids,item_names,item):
    attribute_values=[]
    #print item
    #print item_names
    for item_data in item_names:
        if(type(item_data) is str): # value is dict entry on top level of item dict
            attribute_values.append(item[item_data]) 
        elif(type(item_data) is int): # value is not in item dict, just insert value in db
            attribute_values.append(item_data)
        elif(type(item_data) is dict):
            if(item_data['name'] in item):
                attribute_values.append(item[item_data['name']])
            else:
                attribute_values.append(item_data['NA'])
        elif(type(item_data) is list): # value is a nested entry of item dict
            item_dict=item
            for element in range(len(item_data)-1):
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

def add_data_details(item,timestamp):
    global db_cursor,db_connection
    #attribute_ids
    attribute_ids=['itemID','return_policy','seller_storeowner','seller_business','pictures','start_price','has_reserve','reserve_met','buyitnow_price',
                   'buyer_feedback','buyer_score','shipping_service','insurance','item_description','time_timestamp']

    item_data_names=['ItemID',['ReturnPolicy',['ReturnsAcceptedOption','NA']],['Seller','SellerInfo',['StoreOwner','NA']],['Seller','SellerInfo',['SellerBusinessType','NA']],
                     ['PictureDetails',['PictureURL','NA']],['StartPrice',['value',-1]],['ListingDetails',['HasReservePrice','NA']],['SellingStatus',['ReserveMet','NA']],
                     ['ListingDetails','ConvertedBuyItNowPrice',['value',-1]],['SellingStatus','HighBidder',['PositiveFeedbackPercent',-1]],['SellingStatus','HighBidder',['FeedbackScore',-1]],
                     ['ShippingDetails','ShippingServiceOptions',['ShippingService','NA']],['ShippingDetails','InsuranceDetails',['InsuranceOption','NA']],
                     'Description',['InsuranceDetails',['timestamp',timestamp]]]
    
    query=db_cursor.mogrify('SELECT * FROM itemDataDetails where itemId=%s;',(item['ItemID'],))
    db_cursor.execute(query)
    if(db_cursor.rowcount==0):
        attribute_string,attribute_indices,attribute_values=generate_query_strings(attribute_ids,item_data_names,item)
        if(attribute_values[13] is not None):
            if(len(attribute_values[13])>5000):
                attribute_values[13]=attribute_values[13][0:5000]
        query=db_cursor.mogrify('Insert into itemDataDetails'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
        #print query
        #print len(attribute_values[13][0:20000])
        db_cursor.execute(query)
    db_connection.commit()

def add_data(items,timestamp):
    global db_cursor,db_connection
    attribute_ids=['itemID','title','condition_id','condition_name',
                   'listing_type','listing_bestoffer','listing_buyitnow','listing_buyitnow_price','listing_current_price',
                   'shipping_cost','shipping_type',
                   'payment_transfer','payment_pickup','payment_delivery','payment_paypal','payment_insured',
                   'url_item','url_gallery',
                   'time_timestamp','time_start','time_end','seller_id']
    #===========================================================================
    # item_data_names=['ItemID','Title',
    #                  ['condition','conditionId'],['condition','conditionDisplayName'],
    #                  ['listingInfo','listingType'],['listingInfo','bestOfferEnabled'],['listingInfo','buyItNowAvailable'],['listingInfo','buyItNowPrice',['value',-1.0]],['sellingStatus','currentPrice',['value',-1.0]],
    #                  ['shippingInfo','shippingServiceCost',['value',-1.0]],['shippingInfo','shippingType'],
    #                  ['paymentMethod',['MoneyXferAccepted',0]],['paymentMethod',['CashOnPickup',0]],['paymentMethod',['COD',0]],['paymentMethod',['PayPal',0]],0,
    #                  'viewItemURL','galleryURL',
    #                  ['listingInfo',['timestamp',timestamp]],['listingInfo','startTime'],['listingInfo','endTime']]
    #===========================================================================
    item_data_names=['ItemID','Title',
                     'ConditionID','ConditionDisplayName',
                     'ListingType','BestOfferEnabled',{'name' :'BuyItNowAvailable','NA':'NA'},['buyItNowPrice',['value',-1.0]],['CurrentPrice',['value',-1.0]],
                     ['ShippingCostSummary','ShippingServiceCost',['value',-1.0]],['ShippingCostSummary','ShippingType'],
                     ['PaymentMethods',['MoneyXferAccepted',0]],['PaymentMethods',['CashOnPickup',0]],['PaymentMethods',['COD',0]],['PaymentMethods',['PayPal',0]],0,
                     'ViewItemURLForNaturalSearch',{'name' :'GalleryURL','NA':'NA'},
                     ['listingInfo',['timestamp',timestamp]],'StartTime','EndTime',['Seller','UserID']]
    
    #print item
    for item in items:
        query=db_cursor.mogrify('SELECT * FROM itemData where itemId=%s;',(item['ItemID'],))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            attribute_string,attribute_indices,attribute_values=generate_query_strings(attribute_ids,item_data_names,item)
            query=db_cursor.mogrify('Insert into itemData'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
            db_cursor.execute(query)
    db_connection.commit()
    

def add_item_attributes(item,category,attributes):
    global category_attributes,aspect_filter,db_connection,db_cursor,log_file
    item_id=int(item['ItemID'])
    attribute_names=[attribute['aspectName'] for attribute in attributes]
    attribute_values=[]
    for aspect in category_attributes:
        if(aspect in attribute_names):
            #if(len(attributes[attribute_names.index(aspect)]['aspectValueName'])>64):
            #    print attribute_names.index(aspect)
            #    print attributes[attribute_names.index(aspect)]['aspectValueName']
            #print attributes[attribute_names.index(aspect)]['aspectValueName']
            value=attributes[attribute_names.index(aspect)]['aspectValueName']
            if(type(value) is list):
                value=','.join(value)
            if(len(value)>256):
                value=value[:255]
            attribute_values.append(value)
        else:
            attribute_values.append('')
    try:
        query=db_cursor.mogrify('SELECT * FROM itemAttributes where itemId=%s;',(item_id,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            if(len(aspect_filter)>0):
                aspect_names=[aspect['aspectName']+':|'+aspect['aspectValueName']+'|' for aspect in aspect_filter]
                aspect_names=','.join(aspect_names)
            else:
                aspect_names=''
            attribute_type_id=get_attribute_type_id_from_db(category,category_attributes)
            attribute_ids=['itemID','attributeTypeID']+['attributeValue_'+str(x) for x in range(1,17)]+['query']
            attribute_string='('+','.join(attribute_ids)+')'
            attribute_indices='('+','.join(['%s' for x in range(19)])+')'
            if(len(attribute_values)<16):
                attribute_values+=['']* (16-len(attribute_values))
            attribute_values=[item_id,attribute_type_id]+attribute_values+[aspect_names]
             
            query=db_cursor.mogrify('Insert into itemAttributes'+attribute_string+' VALUES '+attribute_indices,tuple(attribute_values))
            #print query
            db_cursor.execute(query)
    except psycopg2.Error as e:
        f=open(log_file,'a')
        f.write(str(e))
        f.write('\n')
        f.close()  

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
            item_exists=add_items(int(item['itemId']),category)
            if(not item_exists):
                add_item_attributes(item,category,result['aspectFilter'])
                #add_item_data(item,result['timestamp'])
                #add_item_state(item,result['timestamp'])
            #if(item_exists):
            #    print 'item already there'
            #print item['itemId']
        
        #print result
        counter+=1
        filename='items_'+str(category)+'_'+str(counter)+'_0.pkl'
    db_connection.commit()



def get_item_details(filename):
    global trading_api,api_error,db_cursor,log_file
    
    item_ids=pd.read_csv(filename)
    
    #item_ids=item_ids[0:10]
    #[291241712806,301305736929]
    
    max_item_size=40
    current_item=0
    api_error=False
    for (index,item) in item_ids.iterrows():
        api_error=False
        itemID=item['x']
        query=db_cursor.mogrify('SELECT * FROM itemDataDetails where itemId=%s;',(itemID,))
        if(current_item>max_item_size):
            return
        #print query
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            f=open(log_file,'a')
            f.write(str(current_item)+' '+str(itemID)+' '+time.strftime("%H_%M_%S"))
            #f.write('\n')
            f.close()
            query=dict()
            query['DetailLevel']='ItemReturnDescription'
            query['itemID']=itemID
            try:
                response=trading_api.execute('GetItem',query)
            except (ConnectionError,Timeout) as e:
                f=open(log_file,'a')
                f.write(e['response'])
                f.write('\n')
                f.close()
                api_error=True
            except(EbayConnectionError) as e:
                response=e
                #get_invalid_items(item_slice)
                #print response
                #print response[0]
                #print type(response[0])
                f=open(log_file,'a')
                f.write(' '+str(e[0].encode('utf8')))
                f.close()
                api_error=True
            except(EbayConnectionError) as e:
                print 'here2'
                #get_invalid_items(item_slice)
                f=open(log_file,'a')
                f.write(' '+str(e[0].encode('utf8')))
                f.write('\n')
                f.close()
                api_error=True
            if(api_error):
                f=open(log_file,'a')
                f.write('\n')
                continue
            result=response.dict()
            f=open(log_file,'a')
            f.write(' ok')
            f.write('\n')
            #print result
            add_data_details(result['Item'],result['Timestamp'])
            current_item=current_item+1
    

def get_multiple_items(item_ids,category,filename=None):
    global shopping_api,api_error,log_file
    
    slice_begin=0
    slice_end=20
    N=len(item_ids)
    api_error=False
    while(slice_begin<N):
        f=open(log_file,'a')
        f.write(str(slice_begin))
        f.write('\n')
        f.close()
        item_slice=item_ids[slice_begin:slice_end]
        query=dict()
        query['sortOrder']='StartTimeNewest'
        query['itemID']=item_slice
        query['IncludeSelector']='Details,ItemSpecifics,ShippingCosts'
        slice_begin=slice_end
        slice_end+=20
        try:
            response=shopping_api.execute('GetMultipleItems',query)
        except (ConnectionError,Timeout) as e:
            f=open(log_file,'a')
            f.write(str(e))
            f.write('\n')
            f.close()
            api_error=True
        except(EbayConnectionError) as e:
            get_invalid_items(item_slice)
            continue
        result=response.dict()
        #print result
        
        data=result['Item']
        if(type(data) is dict):
            data=[data]
        add_attributes(data,category)
        #print 'adding data'
        add_data(data,result['Timestamp'])
        #print 'adding state'
        add_state(data,result['Timestamp'])
        
        if(filename is not None):
            output = open(filename+'_'+time.strftime("%d_%m_%Y_%H_%M_%S")+str(slice_begin)+'.pkl', 'w')
            pickle.dump({'timestamp':result['Timestamp'],'items':result['Item']},output,-1)
            output.close()
                
    
        #print(e.response.dict())

def remove_invalid_item(item_id):
    global db_connection,db_cursor
    
    query=db_cursor.mogrify('delete FROM itemAttributes where itemId=%s;',(item_id,))
    db_cursor.execute(query)
    query=db_cursor.mogrify('delete FROM itemData where itemId=%s;',(item_id,))
    db_cursor.execute(query)
    query=db_cursor.mogrify('delete FROM itemIds where itemId=%s;',(item_id,))
    db_cursor.execute(query)
    query=db_cursor.mogrify('delete FROM itemState where itemId=%s;',(item_id,))
    db_cursor.execute(query)
    db_connection.commit()

def get_item_history(item):
    global trading_api
    item=321516040735
    query=dict()
    #query['itemID']=item
    response=None
    try:
        response=trading_api.execute('GetAllBidders',query)
    except (ConnectionError,Timeout) as e:
        print(e)
    except(EbayConnectionError) as e:
        a=2
    print(response)
    
    
    

def get_invalid_items(items):
    global finding_api
    
    query=dict()
    for item in items:
        query['itemID']=item
        try:
            shopping_api.execute('GetSingleItem',query)
        except (ConnectionError,Timeout) as e:
            f=open(log_file,'a')
            f.write(str(e))
            f.write('\n')
            f.close()
        except(EbayConnectionError) as e:
            #print e.message
            if 'nicht vorhandene Artikelnummer' in e.message:
                f=open(log_file,'a')
                f.write('removing invalid item '+str(item))
                f.write('\n')
                f.close()
                remove_invalid_item(item)

def add_attributes(items,category):
    global db_connection
    for item in items:
        attributeList=[]
        if('ItemSpecifics' in item):
            attributeList=item['ItemSpecifics']['NameValueList']
        
        if(type(attributeList) is dict):
            attributeList=[attributeList]
        attributes=[{'aspectName':attribute['Name'],'aspectValueName':attribute['Value']} for attribute in attributeList]
        add_item_attributes(item,category,attributes)
    db_connection.commit()

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

error="u'GetMultipleItems: Class: RequestError, Severity: Error, Code: 10.12, Ung\xfcltige Artikelnummer.Ung\xfcltige bzw. nicht vorhandene Artikelnummer.'"

invalid_smartphones=[321527057063L, 261593630199L, 111460989198L, 331323569421L, 251649899348L, 251649897329L, 161426101663L, 261597891063L, 281443288620L, 361054528129L, 151414690690L, 161424692577L, 291241506297L, 171464449456L, 191336639227L, 231337364295L, 321527318914L, 111466612226L, 171465894456L, 331323670525L]


valid_attributes={
    tablets:['Marke','Produktlinie',u'Speicherkapazit\xe4t','Betriebssystem','Farbe'],
    apple_notebooks:['Herstellergarantie', 'Betriebssystem','Marke',u'Bildschirmgr\xf6\xdfe', 'Prozessortyp', 'Arbeitsspeicher', u'Festplattenkapazit\xe4t', 'Prozessorgeschwindigkeit', 'Produktfamilie', 'Erscheinungsjahr','Herstellernummer'],
    #handys_ohne_vertrag:['Marke','Modell',u'Speicherkapazit\xe4t','Farbe','Verbindung','Produktpakete','Vertragslaufzeit']
    handys_ohne_vertrag:['Brand','Model','Memory','Color','Connection','Produktpakete','Vertragslaufzeit']
}


def get_items_by_seller(category,seller):
    global finding_api
    
    query=dict()
    item_filter=[{'name':'SoldItemsOnly','value':'true'}]
    query['ItemFilter']=item_filter
    #query
    query['categoryId']=category
    #query['categoryId']=category
    
    response=finding_api.execute('findCompletedItems',query)
    print response.dict()

# run once in a while...
def get_missing_data(category):
    global api_error,log_file
    
    all_items=get_all_items_from_db(category)
    unknown_items=get_incomplete_data(all_items)
    f=open(log_file,'a')
    f.write('there are '+str(len(unknown_items))+' missing values')
    f.write('\n')
    f.close()
    api_error=True
    while(api_error):
        get_multiple_items(unknown_items,category)
    

def get_incomplete_data(item_ids):
    incomplete_items=[]
    for item_id in item_ids:
        incomplete=False
        query=db_cursor.mogrify('SELECT * FROM itemAttributes where itemId=%s;',(item_id,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            incomplete=True
        query=db_cursor.mogrify('SELECT * FROM itemData where itemId=%s;',(item_id,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            incomplete=True
        query=db_cursor.mogrify('SELECT * FROM itemIds where itemId=%s;',(item_id,))
        db_cursor.execute(query)
        if(db_cursor.rowcount==0):
            incomplete=True
        if(incomplete):
            incomplete_items.append(item_id)
    return incomplete_items


def get_past_items(category,aspect_filter):
    
    item_ids=get_all_items(category,aspect_filter,query_type='findCompletedItems')
    output = open('itemids_completed_'+str(category)+'_'+time.strftime("%d_%m_%Y")+'.pkl', 'w')
    pickle.dump(item_ids,output,-1)
    output.close()
    #output = open('itemids_completed_9355_18_09_2014.pkl', 'r')
    #item_ids=pickle.load(output)
    print 'past items: nr items '+str(len(item_ids))
    
    unknown_items=get_incomplete_data(item_ids)
    
    print 'past items: nr new items '+str(len(unknown_items))
    add_items(unknown_items,category)
    
    #new_items=item_ids
    
    get_multiple_items(unknown_items,category)

def remove_items(items):
    global db_cursor
    try:
        for item in items:
            query=db_cursor.mogrify('DELETE FROM itemids where itemId=%s;',(item,))
            db_cursor.execute(query)
            query=db_cursor.mogrify('DELETE FROM itemattributes where itemId=%s;',(item,))
            db_cursor.execute(query)
            query=db_cursor.mogrify('DELETE FROM itemdata where itemId=%s;',(item,))
            db_cursor.execute(query)
            query=db_cursor.mogrify('DELETE FROM itemstate where itemId=%s;',(item,))
            db_cursor.execute(query)
    except psycopg2.Error as e:
        print e
    db_connection.commit()
    
def remove_items_pickle(filename):
    pickle_file = open(filename, 'r')
    items=pickle.load(pickle_file)
    print 'Removing '+str(len(items))+' items'
    pickle_file.close()
    remove_items(items)

def get_items_pickle(filename):
    pickle_file = open(filename, 'r')
    item_ids=pickle.load(pickle_file)
    pickle_file.close()
    return item_ids

def get_data_pickle(filename):
    
    items={'timestamp':None,'items':list()}
    all_item_ids=[]
    counter=0
    
    root, dirs, files=os.walk('.')
    files = [ fi for fi in files if fi.startswith("filename") ]
    print files
    for f in files:
        if(not os.path.isfile(filename)):
            break
        pkl_file=open(f, 'r')
        item_data=pickle.load(pkl_file)
        pkl_file.close()
        items['timestamp']=item_data['timestamp']
        item_ids=[item['ItemID'] for item in item_data['items']]
        all_item_ids+=item_ids
        items['items'].append(item_data['items'])
    return all_item_ids,items


def load_items():
    a=2

def get_new_items(category,aspect_filter,stop_if_known=True,id_file=None,data_file=None):
    global api_error,log_file
    if(id_file is not None):
        item_ids=get_items_pickle(id_file)
    else:
        api_error=True
        while(api_error):
            item_ids=get_all_items(category,aspect_filter,query_type='findItemsByCategory',stop_if_known=stop_if_known)
    
    f=open(log_file,'a')
    f.write('available items: '+str(len(item_ids)))
    f.write('\n')
    f.close()
    unknown_items=get_incomplete_data(item_ids)
    add_items(unknown_items,category)
    f=open(log_file,'a')
    f.write('new items: '+str(len(unknown_items)))
    f.write('\n')
    f.close()
    api_error=True
    while(api_error):
        get_multiple_items(unknown_items,category)

def update_states(category,time_offset=10):
    global log_file
    
    f=open(log_file,'a')
    f.write('update states')
    f.write('\n')
    f.close()
    active_items=get_active_items_from_db(category)
    today=datetime.datetime.now()
    outdated_items=[]
    for item in active_items:
        time_delta=today-item[1]
        if(time_delta.days>0 or (time_delta.seconds/3600.)>time_offset):
            outdated_items.append(item[0])
    f=open(log_file,'a')
    f.write('active items: '+str(len(active_items)))
    f.write('\n')
    f.write('outdated items: '+str(len(outdated_items)))
    f.write('\n')
    f.close()
    get_multiple_items(outdated_items,category)

def get_product_details(category_id):
    global shopping_api
    
    response = shopping_api.execute('FindProducts', {'CategoryID': category_id})
    print(response.dict())    

if __name__ == '__main__':
    
    cat=int(sys.argv[1])
    #cat=0
    trading_api=None
    finding_api=None
    shopping_api=None
    db_connection=None
    db_cursor=None
    log_file='logs/log_'+str(cat)+'_'+time.strftime("%d_%m_%Y_%H_%M_%S")+'.csv'
    connect_db()
    
    connect_api()
    
    if(cat==4):
        get_item_details('items.csv')
        disconnect_db()
        if(api_error):
            sys.exit(1)
        sys.exit()
    category_aspect_dict={}
    #for category in [apple_notebooks]:
    categories= [apple_notebooks,tablets,handys_ohne_vertrag]
    category=categories[cat]
    category_attributes=valid_attributes[category]
    api_error=True
    while(api_error):
        update_states(category)
    
    if(category==apple_notebooks):
        aspect_filter={}
        get_missing_data(apple_notebooks)
        get_new_items(category,aspect_filter)
        
    elif(category==handys_ohne_vertrag):
        category_aspects={'Modell':['iPhone 6 Plus','iPhone 6','iPhone 5c','iPhone 5s','iPhone 5','Samsung Galaxy S 4','Samsung Galaxy S 5']}
        for m in category_aspects['Modell']:
            aspects={'Modell':m}
            aspect_filter=get_aspect_filter(aspects)
            f=open(log_file,'a')
            f.write(str(aspect_filter))
            f.write('\n')
            f.close()
            get_missing_data(apple_notebooks)
            get_new_items(category,aspect_filter)
        
    elif(category==tablets):
        category_aspects={'Produktlinie':['Galaxy Tab','iPad 1. Generation','iPad 2','iPad 3. Generation','iPad 4. Generation','iPad Air','iPad mini','iPad mini mit Retina Display']}
        #category_aspects={'Produktlinie':['iPad 2','iPad 3. Generation','iPad 4. Generation','iPad Air','iPad mini']}
        for m in category_aspects['Produktlinie']:
            aspects={'Produktlinie':m}
            aspect_filter=get_aspect_filter(aspects)
            f=open(log_file,'a')
            f.write(str(aspect_filter))
            f.write('\n')
            f.close()
            get_missing_data(apple_notebooks)
            get_new_items(category,aspect_filter) 
    
    disconnect_db()
    