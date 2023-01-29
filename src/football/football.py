from threading import Event

import requests

from . import HEADERS

from .models import Table, Matches

class Football:
    def __init__(self) -> None:
        pass

    def get_matches(self, terminate_event: Event) -> None:
        response = requests.get('https://api.football-data.org/v4/competitions/PL/matches?dateFrom=2022-07-01&dateTo=2023-06-30', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            matches = Matches.parse_raw(response.content)

            for match in matches.matches:
                print(f'{match.id} {match.utc_date} {match.home_team.tla} {match.score.full_time.home} - {match.score.full_time.away} {match.away_team.tla} - {match.status}')
        else:
            print(f'Not Allowed: {response.status_code}')

    def get_table(self, terminate_event: Event) -> None:
        response = requests.get('https://api.football-data.org/v4/competitions/PL/standings/', headers=HEADERS)

        if response.status_code == requests.status_codes.codes.ok:
            table = Table.parse_raw(response.content)

            for table_entry in table.standings[0].table:
                print(f'{table_entry.position:02} {table_entry.team.short_name:20} {table_entry.points}')
