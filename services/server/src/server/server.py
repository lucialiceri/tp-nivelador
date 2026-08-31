import socket
import logger
import safe_socket
import lottery
import csv
import io


class Server:
    def __init__(self, server_host: str, server_port: int, output_file: str) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.output_file = output_file

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        lottery_instance = lottery.Lottery(self.output_file)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                line_size = safe_socket.recv_all(client_socket, 4)
                size = int.from_bytes(line_size, "big")
                if size == 0:
                    winners = self._get_winners(lottery_instance, agency_id)
                    logger.info(action, logger.LogResult.success, "winners-amount", len(winners))

                    response = self._serialize_winners(winners)
                    response_size = len(response).to_bytes(4, "big")

                    safe_socket.send_all(client_socket, response_size)
                    safe_socket.send_all(client_socket, response)
                    logger.info(action, logger.LogResult.success, "response-sent")
                    return

                client_message = safe_socket.recv_all(client_socket, size)
                logger.info(action, logger.LogResult.in_progress, "message", client_message)
                bet = self._store_bet(lottery_instance, client_message)

                if agency_id is None:
                    agency_id = bet.agency_id

                message_amount += 1    

        except ConnectionError:
            logger.error(
                action, logger.LogResult.success, "messages-amount", message_amount
            )
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def _get_winners(self, lottery_instance, agency_id):
        if agency_id is None:
            return []

        winners = []
        for bet in lottery_instance.load_bets():
            if lottery_instance.has_won(bet) and bet.agency_id == agency_id:
                winners.append(bet)

        return winners

    def _serialize_winners(self, winners):
        if not winners:
            return b""

        buffer = io.StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
        for bet in winners:
            writer.writerow([bet.agency_id, bet.first_name, bet.last_name, bet.document, bet.birthdate, bet.number])

        return buffer.getvalue().encode("utf-8")

    def _store_bet(self, lottery_instance, client_message):
        message = client_message.decode("utf-8")
        reader = csv.reader([message], quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            if not row:
                continue
            [agency_id, first_name, last_name, document, birthdate, number] = row

            bet = lottery.Bet(int(agency_id), first_name, last_name, int(document), birthdate, int(number))

            lottery_instance.store_bets([bet])
        
        return bet

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
