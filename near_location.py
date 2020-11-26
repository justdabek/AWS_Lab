import random
import math


METERS_IN_DEGREES = 111300.


def near_location(lat, lng, r):
    """ Returns location near the provided one
    :param lat: latitude
    :param lng: longitude
    :param r: radius in meters
    :type lat: float
    :type lng: float
    :type r: int
    """
    u = random.random()
    v = random.random()

    #Convert radius from meters to degrees
    radius = float(r) / METERS_IN_DEGREES
    w = radius * math.sqrt(u)
    t = 2. * math.pi * v

    x = w * math.cos(t) / math.cos(lat)
    y = w * math.sin(t)

    return [ lat+y, lng+x ]
