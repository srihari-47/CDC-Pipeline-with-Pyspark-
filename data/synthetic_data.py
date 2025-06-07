import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


def generate_synthetic_data(num_flts=1000, num_customers=100000, num_bookings=6000000, chunk_size=1000000):
    """This function generates the synthetic data for flights, customer and bookings based on the volume provided
    in the arguments"""
    # Let's generate the flight data
    now = datetime.now()
    stations = ['ORD', 'LAX', 'IAD', 'IAH', 'LHR', 'EWR', 'JFK', 'DEL', 'MAA', 'AMS', 'JAX', 'ACF', 'PTY', 'CUN'
        , 'BOM', 'FRA', 'AUS', 'BRU', 'MVD', 'PHX', 'ATH', 'BCN', 'GEO', 'LGA', 'SEA', 'PIT', 'CLE', 'BOG',
                'DTW', 'MSO', 'BOI', 'BGI', 'CUR', 'PSC', 'GRR', 'MEX', 'CTU', 'XPL', 'DXB', 'CMH', 'OGG']
    flights = []
    for x in range(1, num_flts + 1):
        flt = f"MX{x:04d}"
        departure, arrival = random.sample(stations, 2)
        departure_time = now + timedelta(days=random.randint(1, 180), hours=random.randint(0, 23))
        arrival_time = departure_time + timedelta(hours=random.randint(0, 23))
        flights.append({'flight_id': x,
                        'flight_no': flt,
                        'departure_station': departure,
                        'arrival_station': arrival,
                        'departure_time_utc': departure_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'arrival_time_utc': arrival_time.strftime('%Y-%m-%d %H:%M:%S')})
    flt_df = pd.DataFrame(flights)
    flt_df.to_csv('flights.csv',index=False)
    # Let's generate the customer data
    first_names = ['Rajaji', 'Surya', 'Robert', 'Sanjay', 'Bobby', 'Karthik', 'Naveen', 'mathi', 'Aishwarya',
                   'benjamin', 'Franklin',
                   'Sunaina', 'Kumaran', 'Nitin', 'Harish', 'Viyan', 'Roshan', 'Puneet', 'Aakash', 'Preetam', 'Raju',
                   'Krishna',
                   'Donald', 'Danny', 'Rajkumar', 'Srikanth', 'christoper', 'Alex', 'Amanda', 'Dustin', 'Justin',
                   'Arman',
                   'Alexandar', 'Merab', 'Shavkat', 'Belal', 'Chaini', 'Aljamain', 'Sean', 'Dana', 'Jean', 'Bryce',
                   'Bruce',
                   'Bradley', 'Jamahal', 'Leon', 'Kratos', 'Sumanth', 'Jeeva', 'Hitesh', 'Mahesh', 'Ramesh', 'Logesh',
                   'Dinesh']

    middle_names = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                    'U', 'V', 'W', 'X', 'Y', 'Z']

    last_names = ['Mitchell', 'Silva', 'Makhachev', 'Provazka', 'Periera', 'Ranadheeran', 'Diaz', 'Mcgregor',
                  'Dicaprio',
                  'Porier', 'Gaje', 'Cormier', 'Texeira', 'Kuppana', 'Arora', 'Gowda', 'Singh', 'Yadav', 'Chettiar',
                  'Iyer',
                  'Kumar', 'Patel', 'omalley', 'driscus', 'Duplesis', 'Bhai', 'Theera', 'Chola', 'Pandya', 'Thinakaran',
                  'Maran', 'Nidhi', 'Tendulkar', 'Ganguly', 'Neeraj', 'Bhat', 'Mishra', 'Rao', 'Reddy', 'Nair', 'Menon',
                  'Ramaswamy', 'Naicker', 'Naidu', 'Udaiyar', 'Pillai', 'David', 'Sinha', 'Tata', 'Ambani', 'Birla',
                  'Pilani']
    customers = []
    for x in range(1, num_customers + 1):
        customer_id = x
        first_name = random.choice(first_names)
        a, b = random.sample(middle_names, 2)
        last_name = random.choice(last_names)
        created_at = now - timedelta(days=random.randint(181, 365))
        customers.append({'Customer_ID': customer_id,
                          'first_name': first_name,
                          'middle_name': f'{a}{b}',
                          'last_name': last_name,
                          'full_name': f'{first_name} {a}{b} {last_name}',
                          'email': f'{first_name.lower()}{random.choice([".", "_"])}{customer_id}@gmail.com',
                          'created_at': created_at.strftime("%Y-%m-%d %H:%M:%S")
                          })
    cust_df = pd.DataFrame(customers)
    cust_df.to_csv('customer.csv',index=False)

    # let's generate the booking data. We are splitting the total_bookings needed as separate chunks and processing
    # them individually before having it as single df.
    num_chunks = (num_bookings + chunk_size - 1) // chunk_size
    booking_df = pd.DataFrame([])
    for chunk_idx in range(num_chunks):
        size = min(chunk_size, (num_bookings - (chunk_size * chunk_idx)))
        start = chunk_idx * chunk_size + 1
        book_id = np.arange(start, start + size)
        cust_id = np.random.randint(1, num_customers + 1, size)
        flt_id = np.random.randint(1, num_flts + 1, size)
        created_offsets = np.random.randint(0, 180 * 24 * 3600, size)
        created_at = pd.to_datetime(now) - pd.to_timedelta(created_offsets, unit='s')
        update_offsets = np.random.randint(0, 90 * 24 * 3600, size)
        updated_at = created_at + pd.to_timedelta(update_offsets, unit='s')
        bookings = {'Booking_id': book_id,
                    'Customer_id': cust_id,
                    'flight_id': flt_id,
                    'created_at': created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    'updated_at': updated_at.strftime("%Y-%m-%d %H:%M:%S")}
        if chunk_idx == 0:
            booking_df = pd.DataFrame(bookings)
        else:
            booking_df = pd.concat([booking_df, pd.DataFrame(bookings)], axis=0)
    booking_df.to_csv('bookings.csv',index=False)


if __name__ == '__main__':
    generate_synthetic_data()
