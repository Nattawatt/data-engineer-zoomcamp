import faust

class TaxiRide(faust.Record, validation=True):
    pu_location_id: str
    # passenger_count: int
    # trip_distance: float
    # payment_type: int
    # total_amount: float