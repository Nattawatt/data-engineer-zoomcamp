from typing import List, Dict
from decimal import Decimal
from datetime import datetime


class Ride:
    def __init__(self, arr: List[str]):
        # self.vendor_id = arr[0]
        # self.lpep_pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        # self.lpep_dropoff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        # self.store_and_fwd_flag = arr[3]
        # self.RatecodeID = int(arr[4])
        self.pu_location_id = int(arr[0])

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            # d['vendor_id'],
            # d['lpep_pickup_datetime'][0],
            # d['lpep_dropoff_datetime'][0],
            # d['store_and_fwd_flag'],
            # d['RatecodeID'],
            d['pu_location_id']
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'
