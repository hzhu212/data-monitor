SELECT count(1)
FROM pmc_all_channel_advertising
WHERE event_day = '%(YESTERDAY)s'
;
