# %%
import os
import numpy as np
import pandas as pd
import seaborn as sns
from haversine import haversine
# %%
os.chdir(os.path.dirname(__file__))
# %%
deploy = pd.read_csv('../../data/deployments.csv')
pickup = pd.read_csv('../../data/pickups.csv')
rides  = pd.read_csv('../../data/rides.csv')
# %%
deploy.info()
deploy.describe()
deploy.head()
# %%
pickup.info()
pickup.describe()
pickup.head()
# %%
rides.info()
rides.describe()
rides.head()
rides.apply(pd.Series.nunique)
# %%
events = (pd
    .concat([
        (rides
            .assign(
                kms=lambda df: df.apply(lambda row:
                    haversine((row['start_lat'], row['start_lng']), (row['end_lat'], row['end_lng'])), 1),
                mins=lambda df:
                    (
                      pd.to_datetime(rides.time_ride_end, format="%Y-%m-%d %H:%M:%S.%f %Z", errors='coerce')
                    - pd.to_datetime(rides.time_ride_start,   format="%Y-%m-%d %H:%M:%S.%f %Z", errors='coerce')
                    ).dt.seconds / 60
            )
            .set_index(['ride_id', 'vehicle_id', 'gross_amount', 'kms', 'mins'])[['time_ride_start', 'time_ride_end']].stack()
            .reset_index().rename(columns={'level_5':'event', 0:'ts'})
            .replace({'time_ride_start': 'ride_start', 'time_ride_end': 'ride_end'})
        ),
        (pickup
            .set_index(['task_id', 'vehicle_id'])[['time_task_created', 'time_task_resolved']].stack()
            .reset_index().rename(columns={'task_id': 'pickup_id', 'level_2': 'event', 0:'ts'})
            .replace({'time_task_created': 'pickup_start', 'time_task_resolved': 'pickup_end'})
        ),
        (deploy
            .set_index(['task_id', 'vehicle_id'])[['time_task_created', 'time_task_resolved']].stack()
            .reset_index().rename(columns={'task_id': 'deploy_id', 'level_2': 'event', 0:'ts'})
            .replace({'time_task_created': 'deploy_start', 'time_task_resolved': 'deploy_end'})
        )
    ])
    [['vehicle_id', 'deploy_id', 'pickup_id', 'ride_id', 'gross_amount', 'kms', 'mins', 'event', 'ts']]
    .sort_values(['vehicle_id', 'ts'])
    .reset_index(drop=True)
    # `status`-column to show the latest service event, eg. `deploy_end` means it's available on the street.
    .assign(status=lambda df: df.event.where(~df.event.str.contains('ride')))
    .assign(status=lambda df: df.groupby('vehicle_id').status.ffill())
)
events.head()
# %% [markdown]
# # Check assumptions
# %% [markdown]
# ## Invalid rides
# %%
offending_rides = events[events.ride_id.notnull() & ~(events.status == 'deploy_end') & events.status.notnull()]
offending_rides.groupby(['event', 'status']).size()
offending_rides.describe()
# %% [markdown]
# Quite some rides ending after a service pickup.
#
# Other discrepancies possibly due to both eventual consistency in the source data, and invalid rides as well.
# %% [markdown]
# ## Lack of `gross_amount`?
# %%dd
no_amount = events[events.gross_amount.isnull() & events.ride_id.notnull()]
no_amount.info()
no_amount.describe()
no_amount.ride_id.nunique(), offending_rides.ride_id.nunique(), len(set(no_amount.ride_id) & set(offending_rides.ride_id))
# %% [markdown]
# Half of rides without an amount is also and offending ride.
# Lack of amount is unrelated to distance.
# %% [markdown]
# ## Pricing
# %%
sns.scatterplot(
    data=(events
        [events.kms < 20]
        .drop_duplicates(subset=['ride_id'])
    ),
    x='gross_amount',
    y='kms'
)
# %%
sns.scatterplot(
    data=(events
        [events.kms < 20]
        .drop_duplicates(subset=['ride_id'])
    ),
    x='gross_amount',
    y='mins'
)
# %% [markdown]
# # QR-codes
# %%
(pickup.groupby(['vehicle_id']).qr_code.nunique() == 1).all()
# %% [markdown]
# QR-codes map to vehicle IDs 1:1.
# %% [markdown]
# # Prep data quality checks
# %%
events.select_dtypes('object').apply(lambda c: c.dropna().str.len()).describe()
pickup.qr_code.dropna().map(len).describe()
# %% [markdown]
# All id-columns are made of 20 character-long strings.

# QR-code column made of 6 characters.
