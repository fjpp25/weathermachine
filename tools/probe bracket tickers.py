(venv) xico@weathermachine:~/weathermachine $ cd ~/weathermachine && source venv/bin/activate
python -c "
from market_utils import load_config_env
load_config_env()
import sweep_engine as s, trader
from cities import TRADING_CITIES as R
c = trader.make_client(skip_confirmation=True)
for city, meta in R.items():
    ser = meta.get('high_series')
    tz  = meta['tz']
    if not ser:
        print(f'### {city}: no high_series'); continue
    try:
        m = s._fetch_markets(c, ser, s._today_str(tz))
    except Exception as e:
        print(f'### {city}: fetch error {e}'); continue
    if not m:
        print(f'### {city} ({ser}): 0 markets today'); continue
    print(f'### {city} ({ser})  {len(m)} brackets')
    def _suf(t): return t.split(\"-\")[-1]
    for x in sorted(m, key=lambda b: _suf(b.get('ticker','')) ):
        code = _suf(x.get('ticker',''))
        fs   = x.get('floor_strike')
        cs   = x.get('cap_strike')
        lbl  = x.get('no_sub_title') or x.get('yes_sub_title') or x.get('subtitle')
        print(f'    {code:<8} floor={str(fs):<6} cap={str(cs):<6} | {lbl}')
" 2>&1 | grep -v "KalshiClient ready"
### New York (KXHIGHNY)  6 brackets
    B76.5    floor=76     cap=77     | 76° to 77°
    B78.5    floor=78     cap=79     | 78° to 79°
    B80.5    floor=80     cap=81     | 80° to 81°
    B82.5    floor=82     cap=83     | 82° to 83°
    T76      floor=None   cap=76     | 75° or below
    T83      floor=83     cap=None   | 84° or above
### Chicago (KXHIGHCHI)  6 brackets
    B74.5    floor=74     cap=75     | 74° to 75°
    B76.5    floor=76     cap=77     | 76° to 77°
    B78.5    floor=78     cap=79     | 78° to 79°
    B80.5    floor=80     cap=81     | 80° to 81°
    T74      floor=None   cap=74     | 73° or below
    T81      floor=81     cap=None   | 82° or above
### Miami (KXHIGHMIA)  6 brackets
    B88.5    floor=88     cap=89     | 88° to 89°
    B90.5    floor=90     cap=91     | 90° to 91°
    B92.5    floor=92     cap=93     | 92° to 93°
    B94.5    floor=94     cap=95     | 94° to 95°
    T88      floor=None   cap=88     | 87° or below
    T95      floor=95     cap=None   | 96° or above
### Austin (KXHIGHAUS)  6 brackets
    B100.5   floor=100    cap=101    | 100° to 101°
    B94.5    floor=94     cap=95     | 94° to 95°
    B96.5    floor=96     cap=97     | 96° to 97°
    B98.5    floor=98     cap=99     | 98° to 99°
    T101     floor=101    cap=None   | 102° or above
    T94      floor=None   cap=94     | 93° or below
### Los Angeles (KXHIGHLAX)  6 brackets
    B66.5    floor=66     cap=67     | 66° to 67°
    B68.5    floor=68     cap=69     | 68° to 69°
    B70.5    floor=70     cap=71     | 70° to 71°
    B72.5    floor=72     cap=73     | 72° to 73°
    T66      floor=None   cap=66     | 65° or below
    T73      floor=73     cap=None   | 74° or above
### San Francisco (KXHIGHTSFO)  6 brackets
    B61.5    floor=61     cap=62     | 61° to 62°
    B63.5    floor=63     cap=64     | 63° to 64°
    B65.5    floor=65     cap=66     | 65° to 66°
    B67.5    floor=67     cap=68     | 67° to 68°
    T61      floor=None   cap=61     | 60° or below
    T68      floor=68     cap=None   | 69° or above
### Denver (KXHIGHDEN)  6 brackets
    B90.5    floor=90     cap=91     | 90° to 91°
    B92.5    floor=92     cap=93     | 92° to 93°
    B94.5    floor=94     cap=95     | 94° to 95°
    B96.5    floor=96     cap=97     | 96° to 97°
    T90      floor=None   cap=90     | 89° or below
    T97      floor=97     cap=None   | 98° or above
### Philadelphia (KXHIGHPHIL)  6 brackets
    B77.5    floor=77     cap=78     | 77° to 78°
    B79.5    floor=79     cap=80     | 79° to 80°
    B81.5    floor=81     cap=82     | 81° to 82°
    B83.5    floor=83     cap=84     | 83° to 84°
    T77      floor=None   cap=77     | 76° or below
    T84      floor=84     cap=None   | 85° or above
### Atlanta (KXHIGHTATL)  6 brackets
    B89.5    floor=89     cap=90     | 89° to 90°
    B91.5    floor=91     cap=92     | 91° to 92°
    B93.5    floor=93     cap=94     | 93° to 94°
    B95.5    floor=95     cap=96     | 95° to 96°
    T89      floor=None   cap=89     | 88° or below
    T96      floor=96     cap=None   | 97° or above
### Boston (KXHIGHTBOS)  6 brackets
    B74.5    floor=74     cap=75     | 74° to 75°
    B76.5    floor=76     cap=77     | 76° to 77°
    B78.5    floor=78     cap=79     | 78° to 79°
    B80.5    floor=80     cap=81     | 80° to 81°
    T74      floor=None   cap=74     | 73° or below
    T81      floor=81     cap=None   | 82° or above
### Washington DC (KXHIGHTDC)  6 brackets
    B79.5    floor=79     cap=80     | 79° to 80°
    B81.5    floor=81     cap=82     | 81° to 82°
    B83.5    floor=83     cap=84     | 83° to 84°
    B85.5    floor=85     cap=86     | 85° to 86°
    T79      floor=None   cap=79     | 78° or below
    T86      floor=86     cap=None   | 87° or above
### Houston (KXHIGHTHOU)  6 brackets
    B91.5    floor=91     cap=92     | 91° to 92°
    B93.5    floor=93     cap=94     | 93° to 94°
    B95.5    floor=95     cap=96     | 95° to 96°
    B97.5    floor=97     cap=98     | 97° to 98°
    T91      floor=None   cap=91     | 90° or below
    T98      floor=98     cap=None   | 99° or above
### Phoenix (KXHIGHTPHX)  6 brackets
    B104.5   floor=104    cap=105    | 104° to 105°
    B106.5   floor=106    cap=107    | 106° to 107°
    B108.5   floor=108    cap=109    | 108° to 109°
    B110.5   floor=110    cap=111    | 110° to 111°
    T104     floor=None   cap=104    | 103° or below
    T111     floor=111    cap=None   | 112° or above
### Las Vegas (KXHIGHTLV)  6 brackets
    B101.5   floor=101    cap=102    | 101° to 102°
    B95.5    floor=95     cap=96     | 95° to 96°
    B97.5    floor=97     cap=98     | 97° to 98°
    B99.5    floor=99     cap=100    | 99° to 100°
    T102     floor=102    cap=None   | 103° or above
    T95      floor=None   cap=95     | 94° or below
### Dallas (KXHIGHTDAL)  6 brackets
    B101.5   floor=101    cap=102    | 101° to 102°
    B95.5    floor=95     cap=96     | 95° to 96°
    B97.5    floor=97     cap=98     | 97° to 98°
    B99.5    floor=99     cap=100    | 99° to 100°
    T102     floor=102    cap=None   | 103° or above
    T95      floor=None   cap=95     | 94° or below
### Seattle (KXHIGHTSEA)  6 brackets
    B63.5    floor=63     cap=64     | 63° to 64°
    B65.5    floor=65     cap=66     | 65° to 66°
    B67.5    floor=67     cap=68     | 67° to 68°
    B69.5    floor=69     cap=70     | 69° to 70°
    T63      floor=None   cap=63     | 62° or below
    T70      floor=70     cap=None   | 71° or above
### New Orleans (KXHIGHTNOLA)  6 brackets
    B92.5    floor=92     cap=93     | 92° to 93°
    B94.5    floor=94     cap=95     | 94° to 95°
    B96.5    floor=96     cap=97     | 96° to 97°
    B98.5    floor=98     cap=99     | 98° to 99°
    T92      floor=None   cap=92     | 91° or below
    T99      floor=99     cap=None   | 100° or above
### Minneapolis (KXHIGHTMIN)  6 brackets
    B83.5    floor=83     cap=84     | 83° to 84°
    B85.5    floor=85     cap=86     | 85° to 86°
    B87.5    floor=87     cap=88     | 87° to 88°
    B89.5    floor=89     cap=90     | 89° to 90°
    T83      floor=None   cap=83     | 82° or below
    T90      floor=90     cap=None   | 91° or above
### Oklahoma City (KXHIGHTOKC)  6 brackets
    B93.5    floor=93     cap=94     | 93° to 94°
    B95.5    floor=95     cap=96     | 95° to 96°
    B97.5    floor=97     cap=98     | 97° to 98°
    B99.5    floor=99     cap=100    | 99° to 100°
    T100     floor=100    cap=None   | 101° or above
    T93      floor=None   cap=93     | 92° or below