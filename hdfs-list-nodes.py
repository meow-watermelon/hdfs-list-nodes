#!/usr/bin/env python3

import argparse
import json
import requests
import sys
import time

def get_req_json(url):
    json_dict = None

    try:
        req = requests.get(url, timeout=30)
    except ConnectionError:
        msg = 'ERROR: Failed to make connection to %s' %(url)
        print(msg)
        sys.exit(1)
    except Timeout:
        msg = 'ERROR: Connection timeout on %s' %(url)
        print(msg)
        sys.exit(2)
    except HTTPError:
        msg = 'ERROR: Connection to %s returns %d' %(url, req.status_code)
        print(msg)
        sys.exit(4)
    else:
        json_dict = req.json()

    return json_dict

def get_dn_dict(url):
    dn_dict = dict(zip(['live', 'live_and_decom', 'decom_ing', 'dead', 'dead_and_decom'], [{}, {}, {}, {}, {}]))
    dn_metrics_dict = {}
    jmx_req = url+'/jmx'

    jmx_resp_json = get_req_json(jmx_req)

    if jmx_resp_json != None:
        jmx_resp_list = jmx_resp_json['beans']
        for m in jmx_resp_list:
            if m['name'] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                dn_metrics_dict = m
                break

        if len(dn_metrics_dict) == 0:
            msg = 'ERROR: Failed to find the metric Hadoop:service=NameNode,name=NameNodeInfo'
            print(msg)
            sys.exit(8)

        '''live datanode(s)'''
        live_dn_dict = json.loads(dn_metrics_dict['LiveNodes'])

        if len(live_dn_dict) != 0:
            for dn in live_dn_dict.keys():
                admin_state = live_dn_dict[dn]['adminState']
                last_contact = live_dn_dict[dn]['lastContact']

                if admin_state == 'In Service':
                    dn_dict['live'][dn] = {}
                    dn_dict['live'][dn]['admin_state'] = 'InService'
                    dn_dict['live'][dn]['last_contact'] = last_contact

                if admin_state == 'Decommissioned':
                    dn_dict['live_and_decom'][dn] = {}
                    dn_dict['live_and_decom'][dn]['admin_state'] = 'LiveAndDecommissioned'
                    dn_dict['live_and_decom'][dn]['last_contact'] = last_contact

        '''decommissioning datanode(s)'''
        decom_ing_dn_dict = json.loads(dn_metrics_dict['DecomNodes'])

        if len(decom_ing_dn_dict) != 0:
            for dn in decom_ing_dn_dict.keys():
                dn_dict['decom_ing'][dn] = {}
                dn_dict['decom_ing'][dn]['admin_state'] = 'Decommissioning'
                dn_dict['decom_ing'][dn]['last_contact'] = 'na'

        '''dead datanode(s)'''
        dead_dn_dict = json.loads(dn_metrics_dict['DeadNodes'])

        if len(dead_dn_dict) != 0:
            for dn in dead_dn_dict.keys():
                last_contact = dead_dn_dict[dn]['lastContact']

                if dead_dn_dict[dn]['decommissioned'] == False:
                    dn_dict['dead'][dn] = {}
                    dn_dict['dead'][dn]['admin_state'] = 'Dead'
                    dn_dict['dead'][dn]['last_contact'] = last_contact
                else:
                    dn_dict['dead_and_decom'][dn] = {}
                    dn_dict['dead_and_decom'][dn]['admin_state'] = 'DeadAndDecommissioned'
                    dn_dict['dead_and_decom'][dn]['last_contact'] = last_contact
    else:
        dn_dict = {}

    return dn_dict

if __name__ == '__main__':
    #print(get_dn_dict('http://nn1.internal:50070'))
