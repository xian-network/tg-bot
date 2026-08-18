import json
from datetime import datetime, timedelta
from plugin import TGBFPlugin
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler
import constants as con


class Price(TGBFPlugin):
    """Plugin for displaying token price information using XIAN DEX GraphQL data"""

    async def init(self):
        await self.add_handler(CommandHandler(self.handle, self.price_callback, block=False))

    @TGBFPlugin.logging()
    @TGBFPlugin.send_typing()
    @TGBFPlugin.whitelist()
    async def price_callback(self, update: Update, context: CallbackContext):
        """Handle the price command"""
        # Don't deal with edited messages
        if not update.message:
            return

        # Set default values when no arguments provided
        if not context.args:
            context.args = ["XIAN-XUSDC", "74h"]
        elif len(context.args) == 1:
            # One argument - could be a timeframe or a ticker/pair
            arg = context.args[0]

            # Check if it's a timeframe (ends with h, d, or m and has numbers)
            if (arg.lower().endswith(('h', 'd', 'm')) and
                    any(c.isdigit() for c in arg)):
                # It's a timeframe, use default pair
                context.args = ["XIAN-XUSDC", arg]
            else:
                # It's a ticker or pair
                if '-' not in arg:
                    # It's a ticker, construct ticker-XIAN pair
                    context.args = [f"{arg.upper()}-XIAN", "74h"]
                else:
                    # It's already a pair, add default timeframe
                    context.args = [arg, "74h"]
        elif len(context.args) == 2:
            # If first argument is just a ticker (no dash), construct ticker-XIAN pair
            if '-' not in context.args[0]:
                context.args[0] = f"{context.args[0].upper()}-XIAN"
        else:
            await update.message.reply_text(
                await self.get_info()
            )
            return

        # Parse trading pair symbol (e.g., "XIAN-XUSDC")
        pair_str = context.args[0].upper()

        # Split the pair string (e.g., "XIAN-XUSDC" -> "XIAN", "XUSDC")
        if "-" in pair_str:
            base_symbol, quote_symbol = pair_str.split("-")
        else:
            base_symbol = pair_str
            quote_symbol = "XUSDC"  # Default

        # Default to 1h candles for last 74 hours if no timeframe specified
        interval_minutes = 60  # 1 hour
        limit = 74  # 74 intervals

        # Parse timeframe if provided
        if len(context.args) == 2:
            timeframe = context.args[1].lower()
            if timeframe.endswith('h'):
                try:
                    hours = int(timeframe[:-1])
                    limit = hours
                    interval_minutes = 60  # 1 hour candles
                except ValueError:
                    await update.message.reply_text(
                        f"{con.ERROR} Invalid timeframe format. Use [number]h for hours"
                    )
                    return
            elif timeframe.endswith('d'):
                try:
                    days = int(timeframe[:-1])
                    limit = days
                    interval_minutes = 1440  # Daily candles (24 hours)
                except ValueError:
                    await update.message.reply_text(
                        f"{con.ERROR} Invalid timeframe format. Use [number]d for days"
                    )
                    return
            elif timeframe.endswith('m'):
                try:
                    minutes = int(timeframe[:-1])
                    limit = minutes
                    interval_minutes = 1  # Minute candles
                except ValueError:
                    await update.message.reply_text(
                        f"{con.ERROR} Invalid timeframe format. Use [number]m for minutes"
                    )
                    return
            else:
                await update.message.reply_text(
                    f"{con.ERROR} Invalid timeframe format. Use h for hours, d for days, or m for minutes"
                )
                return

        message = await update.message.reply_text(f"{con.WAIT} Fetching price data...")

        try:
            # Find the pair by symbols
            pair = await self.find_pair_by_symbols(base_symbol, quote_symbol)

            if not pair:
                await message.edit_text(f"{con.ERROR} Trading pair {base_symbol}-{quote_symbol} not found")
                return

            # Determine if base token is token0 or token1
            base_is_token0 = pair.get('token0_symbol', '').upper() == base_symbol.upper()

            # Get swap events
            events = await self.fetch_swap_events(pair['id'])

            if not events:
                await message.edit_text(f"{con.ERROR} No swap events found for {base_symbol}-{quote_symbol}")
                return

            # Process into candles
            candles = self.process_swap_events(events, interval_minutes, limit, base_is_token0)

            if not candles:
                await message.edit_text(
                    f"{con.ERROR} Could not generate price data for {base_symbol}-{quote_symbol}")
                return

            # Calculate current price (last candle close)
            current_price = candles[-1]['close']

            # Calculate price change and percentage
            first_price = candles[0]['open'] if candles else 0
            price_change = current_price - first_price
            price_change_pct = (price_change / first_price) * 100 if first_price else 0

            # Price direction emoji
            direction = con.GREEN if price_change >= 0 else con.RED

            # Calculate 24h volume
            hours_24_ago = datetime.utcnow() - timedelta(hours=24)
            volume_24h = self.calculate_24h_volume_from_trades(events, base_is_token0)

            # Calculate 24h high and low
            candles_24h = [candle for candle in candles if candle['time'] >= hours_24_ago]
            high_24h = max([candle['high'] for candle in candles_24h]) if candles_24h else current_price
            low_24h = min([candle['low'] for candle in candles_24h]) if candles_24h else current_price

            # Format timeframe for display
            if interval_minutes == 1:
                timeframe_str = f"{limit}m"
            elif interval_minutes == 60:
                timeframe_str = f"{limit}h"
            elif interval_minutes == 1440:
                timeframe_str = f"{limit}d"
            else:
                timeframe_str = f"{limit} intervals of {interval_minutes}m"

            # Calculate market cap for XIAN
            market_cap = None
            if base_symbol.upper() == 'XIAN':
                total_supply = 111_111_111  # XIAN total supply
                market_cap = current_price * total_supply

            # Create the price message
            if market_cap is not None:
                # Include market cap
                message_text = (
                    f"{direction} <b>{base_symbol}-{quote_symbol}</b> {timeframe_str}\n\n"
                    f"<code>Price:         {current_price:.6f} {quote_symbol}</code>\n"
                    f"<code>Change:       {price_change:+.6f} {quote_symbol} ({price_change_pct:+.2f}%)</code>\n"
                    f"<code>24h High:      {high_24h:.6f} {quote_symbol}</code>\n"
                    f"<code>24h Low:       {low_24h:.6f} {quote_symbol}</code>\n"
                    f"<code>24h Volume:    {volume_24h:.2f} {base_symbol}</code>\n"
                    f"<code>Market Cap:    {market_cap:,.0f} {quote_symbol}</code>"
                )
            else:
                # No market cap for non-XIAN pairs
                message_text = (
                    f"{direction} <b>{base_symbol}-{quote_symbol}</b> {timeframe_str}\n\n"
                    f"<code>Price:         {current_price:.6f} {quote_symbol}</code>\n"
                    f"<code>Change:       {price_change:+.6f} {quote_symbol} ({price_change_pct:+.2f}%)</code>\n"
                    f"<code>24h High:      {high_24h:.6f} {quote_symbol}</code>\n"
                    f"<code>24h Low:       {low_24h:.6f} {quote_symbol}</code>\n"
                    f"<code>24h Volume:    {volume_24h:.2f} {base_symbol}</code>"
                )

            await message.edit_text(message_text)

        except Exception as e:
            await message.edit_text(f"{con.ERROR} Error fetching price: {e}")
            self.log.error(f"Price error: {e}")
            await self.notify(e)

    async def fetch_pairs(self):
        pairs_query = await self.get_resource("get_pairs.gql", plugin="chart")
        result = await self.fetch_graphql(pairs_query)
        pairs = []

        if not result.get('data', {}).get('allEvents', {}).get('edges'):
            return pairs

        for edge in result['data']['allEvents']['edges']:
            if isinstance(edge['node']['dataIndexed'], str):
                data_indexed = json.loads(edge['node']['dataIndexed'])
            else:
                data_indexed = edge['node']['dataIndexed']

            if isinstance(edge['node']['data'], str):
                pair_data = json.loads(edge['node']['data'])
            else:
                pair_data = edge['node']['data']

            pairs.append({
                'id': pair_data['pair'],
                'token0': data_indexed['token0'],
                'token1': data_indexed['token1']
            })

        return pairs

    def calculate_24h_volume_from_trades(self, events, base_is_token0=True):
        """Calculate 24h volume directly from raw trade events"""
        if not events:
            return 0

        hours_24_ago = datetime.utcnow() - timedelta(hours=24)
        volume_24h = 0

        for edge in events:
            node = edge['node']

            # Parse timestamp
            timestamp_str = node['created']
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

            # Skip trades older than 24 hours
            if timestamp < hours_24_ago:
                continue

            # Parse trade data
            swap_data = json.loads(node['data']) if isinstance(node['data'], str) else node['data']

            amount0_in = float(swap_data.get('amount0In', 0) or 0)
            amount0_out = float(swap_data.get('amount0Out', 0) or 0)
            amount1_in = float(swap_data.get('amount1In', 0) or 0)
            amount1_out = float(swap_data.get('amount1Out', 0) or 0)

            # Calculate volume for this trade in base token
            if amount0_out > 0 and amount1_in > 0:
                # Buying token0 with token1
                volume = amount0_out if base_is_token0 else amount1_in
            elif amount0_in > 0 and amount1_out > 0:
                # Selling token0 for token1
                volume = amount0_in if base_is_token0 else amount1_out
            else:
                volume = 0

            volume_24h += volume

        return volume_24h

    async def fetch_token_symbols(self, token_contracts):
        """Fetch token symbols for multiple token contracts in one query"""
        if not token_contracts:
            return {}

        # Build a combined GraphQL query for all tokens
        query_parts = []
        for i, token in enumerate(token_contracts):
            query_parts.append(f"""
                symbol_{i}: allStates(condition: {{key: "{token}.metadata:token_symbol"}}) {{
                    nodes {{
                        key
                        value
                    }}
                }}
            """)

        full_query = f"query GetTokensMetadata {{{' '.join(query_parts)}}}"

        result = await self.fetch_graphql(full_query)

        token_symbols = {}
        for i, token in enumerate(token_contracts):
            symbol_data = result.get('data', {}).get(f'symbol_{i}', {}).get('nodes', [])
            if symbol_data and len(symbol_data) > 0:
                value = symbol_data[0].get('value')
                if value:
                    symbol = value if isinstance(value, str) else json.dumps(value)
                    token_symbols[token] = symbol

        return token_symbols

    async def find_pair_by_symbols(self, base_symbol, quote_symbol='XUSDC'):
        """Find a pair by base and quote symbols"""
        # Get all pairs
        pairs = await self.fetch_pairs()

        if not pairs:
            return None

        # Get unique token contracts
        token_contracts = set()
        for pair in pairs:
            token_contracts.add(pair['token0'])
            token_contracts.add(pair['token1'])

        # Get token symbols
        token_symbols = await self.fetch_token_symbols(list(token_contracts))

        # Find the pair with matching symbols
        for pair in pairs:
            token0_symbol = token_symbols.get(pair['token0'], '').upper()
            token1_symbol = token_symbols.get(pair['token1'], '').upper()

            # Check for match in either direction
            if (token0_symbol == base_symbol.upper() and token1_symbol == quote_symbol.upper()) or \
                    (token1_symbol == base_symbol.upper() and token0_symbol == quote_symbol.upper()):
                # Store the symbols for later use
                pair['token0_symbol'] = token_symbols.get(pair['token0'], pair['token0'])
                pair['token1_symbol'] = token_symbols.get(pair['token1'], pair['token1'])
                return pair

        return None

    async def fetch_swap_events(self, pair_id):
        """Fetch all swap events for a specific pair."""
        query = await self.get_resource("get_swap_events.gql", plugin="chart")
        after = None
        events = []

        while True:
            result = await self.fetch_graphql(query, {
                "pairId": pair_id,
                "createdAfter": "1970-01-01T00:00:00Z",
                "first": 500,
                "after": after,
            })
            connection = result.get("data", {}).get("allEvents", {})
            events.extend(connection.get("edges", []))
            page_info = connection.get("pageInfo", {})

            if not page_info.get("hasNextPage"):
                return events

            next_cursor = page_info.get("endCursor")
            if not next_cursor or next_cursor == after:
                raise RuntimeError("GraphQL pagination did not advance")
            after = next_cursor

    def process_swap_events(self, events, interval_minutes, limit, base_is_token0=True):
        """Convert swap events to candlestick data for price calculation"""
        if not events:
            return []

        # Extract trades
        trades = []
        for edge in events:
            node = edge['node']

            # Parse data
            data_indexed = json.loads(node['dataIndexed']) if isinstance(node['dataIndexed'], str) else node[
                'dataIndexed']
            swap_data = json.loads(node['data']) if isinstance(node['data'], str) else node['data']

            # Parse the timestamp (ensure UTC)
            timestamp_str = node['created']
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

            # Calculate price
            amount0_in = float(swap_data.get('amount0In', 0) or 0)
            amount0_out = float(swap_data.get('amount0Out', 0) or 0)
            amount1_in = float(swap_data.get('amount1In', 0) or 0)
            amount1_out = float(swap_data.get('amount1Out', 0) or 0)

            price = None
            volume = 0

            if amount0_out > 0 and amount1_in > 0:
                # Buying token0 with token1
                price = amount1_in / amount0_out
                # Volume in base token
                volume = amount0_out if base_is_token0 else amount1_in
            elif amount0_in > 0 and amount1_out > 0:
                # Selling token0 for token1
                price = amount1_out / amount0_in
                # Volume in base token
                volume = amount0_in if base_is_token0 else amount1_out

            # Invert price if base is token1 instead of token0
            if price is not None and not base_is_token0:
                price = 1 / price
                # Volume is already correctly calculated based on base_is_token0

            if price:
                trades.append({
                    'timestamp': timestamp,
                    'price': price,
                    'volume': volume
                })

        # Sort by timestamp (oldest first)
        trades.sort(key=lambda x: x['timestamp'])

        if not trades:
            return []

        # Get time boundaries for candles
        current_time = datetime.utcnow()
        start_time = current_time - timedelta(minutes=interval_minutes * limit)

        # Round to interval boundaries
        rounded_start = start_time.replace(
            minute=(start_time.minute // interval_minutes) * interval_minutes,
            second=0,
            microsecond=0
        )

        # Use current time for the end boundary to include in-progress candle
        current_interval_start = current_time.replace(
            minute=(current_time.minute // interval_minutes) * interval_minutes,
            second=0,
            microsecond=0
        )

        # Calculate the next interval after current for proper boundaries
        next_interval = current_interval_start + timedelta(minutes=interval_minutes)

        # Generate all intervals
        intervals = []
        current = rounded_start
        while current <= current_interval_start:
            intervals.append(current)
            current += timedelta(minutes=interval_minutes)

        # Add one more interval for the future (needed for proper interval boundaries)
        intervals.append(next_interval)

        # Process candles
        candles = []
        previous_close = None

        for i in range(len(intervals) - 1):
            interval_start = intervals[i]
            interval_end = intervals[i + 1]

            # For the current in-progress interval, use actual current time as boundary
            actual_end = current_time if interval_start == current_interval_start else interval_end

            # Find trades in this interval
            interval_trades = [t for t in trades
                               if interval_start <= t['timestamp'] < actual_end]

            if interval_trades:
                if previous_close is None:
                    # First candle with trades
                    candle = {
                        'time': interval_start,
                        'open': interval_trades[0]['price'],
                        'high': max(t['price'] for t in interval_trades),
                        'low': min(t['price'] for t in interval_trades),
                        'close': interval_trades[-1]['price'],
                        'volume': sum(t['volume'] for t in interval_trades)
                    }
                    previous_close = candle['close']
                else:
                    # Subsequent candle with trades
                    candle = {
                        'time': interval_start,
                        'open': previous_close,
                        'high': max([previous_close] + [t['price'] for t in interval_trades]),
                        'low': min([previous_close] + [t['price'] for t in interval_trades]),
                        'close': interval_trades[-1]['price'],
                        'volume': sum(t['volume'] for t in interval_trades)
                    }
                    previous_close = candle['close']
            elif previous_close is not None:
                # Empty candle - maintain price from previous candle
                candle = {
                    'time': interval_start,
                    'open': previous_close,
                    'high': previous_close,
                    'low': previous_close,
                    'close': previous_close,
                    'volume': 0
                }
            else:
                # Skip intervals until we find the first trade
                continue

            candles.append(candle)

        return candles
