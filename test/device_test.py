import aiohttp
import json
import asyncio
import argparse

async def device_call(address, port=8080, method='call', device_id=0, term_id=0, item_id=0, value=None):
    try:
        call_dict = {'device_id': device_id, 'term_id': term_id, 'item_id': item_id, 'value': value}
        uri = 'http://{}:{}/api/v1/device_{}'.format(address, port, method)
        print('send %s to %s' % (call_dict, uri))
        async with aiohttp.post(uri, data=json.dumps(call_dict)) as r:
            if r.status == 200:
                rst = await r.json()
                print('SUCCESS!')
                print('result =', rst)
            else:
                rst = await r.text()
                print('ERROR! code =', r.status)
                print('err_msg =', rst)
    except Exception as e:
        print('ERROR: %s', repr(e))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PyDataColl接口调试程序')
    parser.add_argument('-a', type=str, default='127.0.0.1', help='采集程序IP地址,默认127.0.0.1')
    parser.add_argument('-p', type=int, default=8080, help='采集程序监听端口,默认8080')
    parser.add_argument('-m', type=str, default='call', help='调用接口, 设备招测: -m=call,设备控制: -m=ctrl')
    parser.add_argument('-d', type=str, default='0', help='device_id, -d=1')
    parser.add_argument('-t', type=str, default='0', help='term_id, -t=1')
    parser.add_argument('-i', type=str, default='0', help='item_id, -d=1')
    parser.add_argument('-v', type=float, default=0, help='value, -v=123.4')
    args = parser.parse_args()
    asyncio.get_event_loop().run_until_complete(device_call(
        address=args.a, port=args.p, method=args.m, device_id=args.d, term_id=args.t, item_id=args.i, value=args.v
    ))
