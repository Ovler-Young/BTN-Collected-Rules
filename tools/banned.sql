WITH DeduplicatedData AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY peer_ip, peer_port, torrent_identifier, user_id 
                           ORDER BY 
                               uploaded DESC, 
                               downloaded DESC) AS row_num
    FROM 
        btn3.bans
    WHERE 
        (uploaded > 0 OR downloaded > 0)
        AND peer_id NOT LIKE '%XL%'
        AND peer_id NOT LIKE '%tT%'
        AND client_name NOT LIKE 'Xunlei%'
        AND insert_time >= now() - INTERVAL 12 HOUR
),
FilteredData AS (
    SELECT 
        insert_time,
        user_id,
        replaceRegexpOne(IPv6NumToString(peer_ip), '^::ffff:', '') AS ip,
        uploaded,
        rt_upload_speed,
        downloaded,
        rt_download_speed,
        peer_id,
        client_name,
        peer_port,
        module,
        rule
    FROM 
        DeduplicatedData
    WHERE 
        row_num = 1
)

SELECT * FROM FilteredData;
