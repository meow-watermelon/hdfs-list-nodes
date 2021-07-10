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
    except requests.exceptions.RequestException:
        msg = 'ERROR: Failed to make connection to %s' %(url)
        print(msg)
        sys.exit(1)

    try:
        json_dict = req.json()
    except:
        msg = 'ERROR: Failed to parse JSON output'
        print(msg) 
        sys.exit(2)

    return json_dict

def get_dn_dict(url):
    dn_dict = dict(zip(['live', 'live_and_decom', 'decom_ing', 'dead', 'dead_and_decom'], [{}, {}, {}, {}, {}]))
    dn_metrics_dict = {}
    jmx_req = url+'/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo'

    jmx_resp_json = get_req_json(jmx_req)

    if jmx_resp_json != None:
        dn_metrics_dict = jmx_resp_json['beans'][0]

        if len(dn_metrics_dict) == 0:
            msg = 'ERROR: Failed to find the metric Hadoop:service=NameNode,name=NameNodeInfo'
            print(msg)
            sys.exit(4)

        # live datanode(s)
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

        # decommissioning datanode(s)
        decom_ing_dn_dict = json.loads(dn_metrics_dict['DecomNodes'])

        if len(decom_ing_dn_dict) != 0:
            for dn in decom_ing_dn_dict.keys():
                dn_dict['decom_ing'][dn] = {}
                dn_dict['decom_ing'][dn]['admin_state'] = 'Decommissioning'
                dn_dict['decom_ing'][dn]['last_contact'] = 'na'

        # dead datanode(s)
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

def get_dn_state_output(url, state):
    dn_state_dict = get_dn_dict(url)
    dn_state_list = []
    current_time = int(time.time())

    if len(dn_state_dict) == 0:
        msg = 'WARNING: No available state data gathered'
        print(msg)
        sys.exit(8)

    if state == 'live':
        if len(dn_state_dict['live']) != 0:
            for dn in dn_state_dict['live'].keys():
                admin_state = dn_state_dict['live'][dn]['admin_state']
                last_contact = dn_state_dict['live'][dn]['last_contact']

                timestamp = time.asctime(time.gmtime(current_time - last_contact))

                output = '%s\t%s\t%s\t%d' %(dn, admin_state, timestamp, last_contact)
                dn_state_list.append(output)
                print(output)
    elif state == 'live-and-decom':
        if len(dn_state_dict['live_and_decom']) != 0:
            for dn in dn_state_dict['live_and_decom'].keys():
                admin_state = dn_state_dict['live_and_decom'][dn]['admin_state']
                last_contact = dn_state_dict['live_and_decom'][dn]['last_contact']

                timestamp = time.asctime(time.gmtime(current_time - last_contact))

                output = '%s\t%s\t%s\t%d' %(dn, admin_state, timestamp, last_contact)
                dn_state_list.append(output)
                print(output)
    elif state == 'decom-ing':
        if len(dn_state_dict['decom_ing']) != 0:
            for dn in dn_state_dict['decom_ing'].keys():
                admin_state = dn_state_dict['decom_ing'][dn]['admin_state']
                last_contact = dn_state_dict['decom_ing'][dn]['last_contact']

                output = '%s\t%s\t%s\t%s' %(dn, admin_state, 'na', last_contact)
                dn_state_list.append(output)
                print(output)
    elif state == 'dead':
        if len(dn_state_dict['dead']) != 0:
            for dn in dn_state_dict['dead'].keys():
                admin_state = dn_state_dict['dead'][dn]['admin_state']
                last_contact = dn_state_dict['dead'][dn]['last_contact']

                timestamp = time.asctime(time.gmtime(current_time - last_contact))

                output = '%s\t%s\t%s\t%d' %(dn, admin_state, timestamp, last_contact)
                dn_state_list.append(output)
                print(output)
    elif state == 'dead-and-decom':
        if len(dn_state_dict['dead_and_decom']) != 0:
            for dn in dn_state_dict['dead_and_decom'].keys():
                admin_state = dn_state_dict['dead_and_decom'][dn]['admin_state']
                last_contact = dn_state_dict['dead_and_decom'][dn]['last_contact']

                timestamp = time.asctime(time.gmtime(int(time.time()) - last_contact))

                output = '%s\t%s\t%s\t%d' %(dn, admin_state, timestamp, last_contact)
                dn_state_list.append(output)
                print(output)
    else:
        msg = 'ERROR: Unrecognized parameter'
        print(msg)
        sys.exit(16)

if __name__ == '__main__':
    # set up args
    parser = argparse.ArgumentParser(description='HDFS DataNode State List', epilog='Display Format: DataNode<tab>State<tab>LastContactTimestamp<tab>LastContactSecond(s)')
    parser.add_argument('-u', '--url', type=str, required=True, help='full namenode url. <http(s)://namenode-hostname:http-port>')
    parser.add_argument('-s', '--state', type=str, required=True, help='datanode state. <live|live-and-decom|decom-ing|dead|dead-and-decom>')
    args = parser.parse_args()

    # execution
    get_dn_state_output(args.url, args.state)
