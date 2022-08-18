"""
    Countries.
"""

import json
import time
import random
import datetime
import requests
import multiprocessing
from unidecode import unidecode

import regex

from decorators import try_n_times
from database_connection import DatabaseConnection
from config.config import DATA_PATH, PLAYERS, INIT_TIME_LIMIT, NUMBER_OF_QUESTIONS, \
                          RESPONSE_TIME_LIMIT, N_PAR, TOKENS, CHAT_IDS

class Countries():
    """Clase con la implementacion del juego."""

    def __init__(self, data_path, players, init_time_limit, number_of_questions, response_time_limit, 
                 n_par, tokens, chat_ids):

        self.data_path           = data_path
        self.players             = players
        self.init_time_limit     = init_time_limit
        self.number_of_questions = number_of_questions
        self.response_time_limit = response_time_limit
        self.n_par               = n_par
        self.tokens              = tokens
        self.chat_ids            = chat_ids
        random.shuffle(self.players)

    def run_countries(self):
        """Metodo que ejecuta la app y se asegura de que se cierre la conexion con la base de datos."""

        try:
            self._run_countries()
        finally:
            if hasattr(self, 'database_conn'):
                self.database_conn.close_connection()

    def _run_countries(self):
        """Metodo principal que ejecuta la aplicacion."""

        countries = self._load_data()

        starmap_args = [(countries, player) for player in self.players]
        pool         = multiprocessing.Pool(processes=self.n_par)
        results      = pool.starmap(self._player_execution, starmap_args)

        self.database_conn = DatabaseConnection()
        self.cur           = self.database_conn.get_cursor()

        self._add_players_to_database()
        self._update_player_points(results)
        self._generate_table_backup()
        self._send_closing_message()

    def _player_execution(self, countries, player):
        """Metodo para ejecutar el juego para un unico jugador y paralelizarlo."""

        token   = self.tokens[player]
        chat_id = self.chat_ids[player]
        self._send_welcome_message(token, chat_id, player)

        if self._wait_for_response(token, self.init_time_limit) is None:
            self._send_telegram_msg(token, chat_id, "Ha perdido su turno de hoy. Hasta mañana!")
            return None

        already_asked = []
        daily_points  = 0

        for i in range(self.number_of_questions):

            country = random.choice([country for country in countries if country['Country'] not in already_asked])
            already_asked.append(country['Country'])

            if random.randint(0, 3) < 3:
                self._send_telegram_msg(token, chat_id, f"{i + 1}. Capital de {country['Country']}?")
                answer = country['Capital']
            else:
                self._send_telegram_msg(token, chat_id, f"{i + 1}. País cuya capital es {country['Capital']}?")
                answer = country['Country']

            response = self._wait_for_response(token, self.response_time_limit)
            if response is None:
                self._send_telegram_msg(token, chat_id, "Se acabó el tiempo. Conteste más rápido la próxima vez.")
            else:
                daily_points = self._check_country_response(token, chat_id, response, country, answer, daily_points)

            self._send_telegram_msg(token, chat_id, "Continente?")

            response = self._wait_for_response(token, self.response_time_limit)
            if response is None:
                self._send_telegram_msg(token, chat_id, "Se acabó el tiempo. Conteste más rápido la próxima vez.")
            else:
                daily_points = self._check_continent_response(token, chat_id, response, country, country['Continent'], daily_points)

        self._send_telegram_msg(token, chat_id, f"FIN! Hoy ha sumado {daily_points} puntos.")

        return (player, daily_points)

    def _load_data(self):
        """Metodo para cargar el fichero json de datos."""

        with open(self.data_path) as f:
            countries = json.load(f)

        return countries

    @try_n_times(n=10)
    def _send_telegram_msg(self, token, chat_id, message):
        """Metodo para enviar un mensaje a un chat de Telegram."""

        return requests.post(f'https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}')

    @try_n_times(n=10)
    def _get_telegram_updates(self, token):
        """Metodo para obtener los updates de un chat de Telegram."""

        return requests.post(f'https://api.telegram.org/bot{token}/getUpdates')

    def _wait_for_response(self, token, time_limit):
        """Metodo que espera a la respuesta con el limite de tiempo determinado."""

        init_time = time.time()
        response  = None

        while response is None and time.time() - init_time <= time_limit:
            api_response = self._get_telegram_updates(token).json()
            if updates := api_response['result']:
                valid_updates = [update for update in updates if update['message']['date'] >= init_time]
                if valid_updates:
                    response = valid_updates[-1]
            time.sleep(0.5)

        return response

    def _send_welcome_message(self, token, chat_id, player):
        """Metodo para enviar el mensaje inicial."""

        self._send_telegram_msg(token, chat_id, 
            f"=========================\nCOUNTRIES ({datetime.datetime.today().strftime('%d-%m-%Y')})\n=========================\n\n" + 
            f"Hola {player}, bienvenido de nuevo a Countries!\n" + 
            f"Responda a este mensaje para comenzar.\n" + 
            f"Cualquier respuesta iniciará el juego.\n" + 
            f"Si en {self.init_time_limit} segundos no responde perderá su turno de hoy."
        )

    def _check_country_response(self, token, chat_id, response, country, answer, points):
        """Metodo que comprueba si la respuesta a la pregunta de la capital es correcta."""

        unicode_answer   = unidecode(f"{answer.lower()}")
        unicode_response = unidecode(f"{response['message']['text'].lower()}")

        if regex.match(f'({unicode_answer}){{e<=1}}$', unicode_response):
            self._send_telegram_msg(token, chat_id, "Correcto! Ha ganado 3 puntos.")
            points += 3
        else:
            self._send_telegram_msg(token, chat_id, f'Incorrecto. La capital de {country["Country"]} es {country["Capital"]}.')

        return points

    def _check_continent_response(self, token, chat_id, response, country, answer, points):
        """Metodo que comprueba si la respuesta a la pregunta de la capital es correcta."""

        unicode_answer   = unidecode(f"{answer.lower()}")
        unicode_response = unidecode(f"{response['message']['text'].lower()}")

        if regex.match(f'({unicode_answer}){{e<=1}}$', unicode_response):
            self._send_telegram_msg(token, chat_id, "Correcto! Ha ganado 1 punto.")
            points += 1
        else:
            self._send_telegram_msg(token, chat_id, f'Incorrecto. {country["Country"]} está en {country["Continent"]}.')

        return points

    def _add_players_to_database(self):
        """Metodo para incluir los jugadores que falten en la tabla de clasificacion."""

        self.cur.execute("SELECT jugador FROM classification")
        players_in_table = [item[0] for item in self.cur.fetchall()]

        values_to_insert = ""
        for player in self.players:
            if player not in players_in_table:
                values_to_insert += f"('{player}', 0), "

        if values_to_insert:
            self.cur.execute(f"INSERT INTO classification (jugador, puntos) VALUES {values_to_insert[:-2]};")

    def _update_player_points(self, results):
        """Metodo para actualizar los puntos del jugador."""

        for result in results:
            if result is not None:
                self.cur.execute(f"SELECT puntos FROM classification WHERE jugador = '{result[0]}'")
                self.cur.execute(f"UPDATE classification SET puntos = '{self.cur.fetchone()[0] + result[1]}' WHERE jugador = '{result[0]}'")

    def _generate_table_backup(self):
        """Metodo para crear un backup de la tabla de clasificacion."""

        self.cur.execute("DROP TABLE IF EXISTS classification_backup;")
        self.cur.execute("CREATE TABLE classification_backup AS (SELECT * FROM classification WHERE 1=1);")

    def _send_closing_message(self):
        """Metodo para enviar el mensaje con la clasificacion."""

        classification     = self._get_classification()
        classification_msg = self._get_classification_msg(classification)
        for player in self.players:
            token   = self.tokens[player]
            chat_id = self.chat_ids[player]
            self._send_telegram_msg(token, chat_id, 
                f'CLASIFICACIÓN\n-----------------------------\n{classification_msg[:-1]}\n-----------------------------\n\n' + 
                f'{classification[0][0]} va en cabeza.\n{classification[-1][0]} ponte las pilas...\n\n' + 
                f'Hasta manaña!\n\n' + 
                f"========================="
            )

    def _get_classification(self):
        """Metodo para obtener la clasificacion."""

        self.cur.execute("SELECT jugador, puntos FROM classification ORDER BY puntos DESC")

        return self.cur.fetchall()

    def _get_classification_msg(self, classification):
        """Metodo para obtener el mensaje de la clasificacion para Telegram."""

        return "".join(f'{i + 1}. {player} --> {points} puntos\n' for i, (player, points) in enumerate(classification))

if __name__ == '__main__':

    countries = Countries(
        DATA_PATH, PLAYERS, INIT_TIME_LIMIT, NUMBER_OF_QUESTIONS, 
        RESPONSE_TIME_LIMIT, N_PAR, TOKENS, CHAT_IDS
    )
    countries.run_countries()
