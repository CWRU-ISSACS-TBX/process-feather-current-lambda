import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import decimal
from decimal import Decimal
from enum import Enum

# laser cutter runs on 240 volts
VOLTS = 240
# thresholds for in-use/idle/off (amps)
THRESHOLD_USING = 5
THRESHOLD_ON = 1

class State(Enum):
    OFF = 0
    IDLE = 1
    USING = 2

db = boto3.resource('dynamodb')
dataMon = db.Table('dataMonitoring')
devAssoc = db.Table('deviceAssociation-dev')

def get_association_info(dev_eui):
    info = devAssoc.get_item(
        Key={'recordingDeviceId': dev_eui},
        ProjectionExpression="timeStampInterval, ports[0].deviceId")
    interval = info['Item']['timeStampInterval']
    machineId = info['Item']['ports'][0]['deviceId']
    return (interval, machineId)

def get_last_entry(machineId):
    scan_kwargs = {
        'FilterExpression': Key('deviceId').eq(machineId)
    }
    response = dataMon.scan(**scan_kwargs)
    latest_entry = None
    for entry in response.get('Items', []):
        if not latest_entry:
            latest_entry = entry
        else:
            if entry['timeStamp'] > latest_entry['timeStamp']:
                latest_entry = entry
    return latest_entry

def calc_useful_info(new_timestamp, new_energy, interval, state, recent_entry=None):
    if recent_entry is None:
        counted = 1
        total_energy = Decimal(new_energy) / Decimal(1)
        average = total_energy
        total_idle = total_off = total_used = 0
        minutes_diff = Decimal(interval)
    else:
        # subtract timestamps to get amount of time for this packet
        #new_time = datetime.fromisoformat(new_timestamp)
        #last_time = datetime.fromisoformat(recent_entry['timeStamp'])
        #time_diff = new_time - last_time
        #minutes_diff = Decimal(time_diff.total_seconds()) / 60
        # just use interval from db
        minutes_diff = Decimal(interval)

        counted = recent_entry['usefulInformation']['counted'] + 1
        total_energy = Decimal(recent_entry['usefulInformation']['totalPower']) + Decimal(new_energy)
        average = total_energy / counted

        total_idle = recent_entry['usefulInformation']['totalTimeIdle']
        total_off = recent_entry['usefulInformation']['totalTimeOff']
        total_used = recent_entry['usefulInformation']['totalTimeUsed']

    if state is State.USING:
        total_used += minutes_diff
    elif state is State.IDLE:
        total_idle += minutes_diff
    else:
        total_off += minutes_diff

    return {
        'average': average,
        'counted': counted,
        'totalPower': total_energy,
        'totalTimeIdle': total_idle,
        'totalTimeOff': total_off,
        'totalTimeUsed': total_used
    }

def lambda_handler(event, context):

    if event['decoded']['status'] != 'success':
        return {
            'success': False,
            'reason': 'failed to decode',
        }

    payload = event['decoded']['payload']

    # unique lora identifier
    device_eui = event['dev_eui']

    # get time interval (minutes) and machine ID (serial num)
    # from deviceAssociation table
    interval, machineId = get_association_info(device_eui)

    # get reported current values
    avg_current = Decimal(payload[0]['value'])
    min_current = Decimal(payload[1]['value']) # currently unused
    max_current = Decimal(payload[2]['value']) # currently unused

    if avg_current > THRESHOLD_USING:
        state = State.USING
    elif avg_current > THRESHOLD_ON:
        state = State.IDLE
    else:
        state = State.OFF

    # convert current to power + energy
    avg_power = avg_current * VOLTS
    energy = avg_power * interval / 60

    # timestamp stored in ISO-8601
    timestamp = event['reported_at']
    date_string = datetime.fromtimestamp(timestamp).isoformat()

    # get most recent dataMonitoring entry to calculate cumulative values
    recent_entry = get_last_entry(machineId)

    # calculate usefulInformation from most recent entry and new data
    if recent_entry is not None:
        usefulInfo = calc_useful_info(date_string, energy, interval, state, recent_entry=recent_entry)
    else:
        usefulInfo = calc_useful_info(date_string, energy, interval, state)

    # save to dataMonitoring table
    row = {
        'deviceId': machineId,
        'energy': energy,
        'timeStamp': date_string,
        'usefulInformation': usefulInfo,
    }
    
    # finish up
    result = {'success': True}
    put_result = dataMon.put_item(Item=row)
    result['result'] = put_result
    
    if put_result['ResponseMetadata']['HTTPStatusCode'] != 200:
        result['success'] = False
    
    return result
