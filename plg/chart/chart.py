import io
import json

import pandas as pd
import constants as con
import plotly.io as pio
import plotly.graph_objects as go

from telegram import Update
from telegram.ext import CallbackContext, CommandHandler
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from plugin import TGBFPlugin


class Chart(TGBFPlugin):
    """Plugin for displaying candlestick charts using XIAN DEX GraphQL data"""

    async def init(self):
        # Add the command handler
        await self.add_handler(CommandHandler(self.handle, self.chart_callback, block=False))

        # Create database tables if they don't exist
        await self.create_db_tables()

        # Perform initial data loading
        await self.load_initial_data()

        # Set up background jobs
        self.run_repeating(self.update_trades_job, interval=60)  # Every minute
        self.run_repeating(self.update_pairs_job, interval=303)  # Every 5 minutes
        self.run_repeating(self.update_tokens_job, interval=306)  # Every 5 minutes

    async def create_db_tables(self):
        """Create required database tables if they don't exist"""

        # Create pairs table
        if not await self.table_exists("chart_pairs"):
            pairs_sql = """
                        CREATE TABLE chart_pairs \
                        ( \
                            id         TEXT PRIMARY KEY, \
                            token0     TEXT NOT NULL, \
                            token1     TEXT NOT NULL, \
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) \
                        """
            await self.exec_sql(pairs_sql)
            self.log.info("Created chart_pairs table")

        # Create tokens table
        if not await self.table_exists("chart_tokens"):
            tokens_sql = """
                         CREATE TABLE chart_tokens \
                         ( \
                             address    TEXT PRIMARY KEY, \
                             symbol     TEXT NOT NULL, \
                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                             updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                         ) \
                         """
            await self.exec_sql(tokens_sql)
            self.log.info("Created chart_tokens table")

        # Create trades table
        if not await self.table_exists("chart_trades"):
            trades_sql = """
                         CREATE TABLE chart_trades \
                         ( \
                             id         TEXT PRIMARY KEY, \
                             pair_id    TEXT      NOT NULL, \
                             timestamp  TIMESTAMP NOT NULL, \
                             data       TEXT      NOT NULL, \
                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                             FOREIGN KEY (pair_id) REFERENCES chart_pairs (id)
                         ) \
                         """
            await self.exec_sql(trades_sql)
            self.log.info("Created chart_trades table")

    async def load_initial_data(self):
        """Load initial data into the database on startup"""
        try:
            # First load pairs
            pairs = await self.fetch_pairs_from_graphql()
            if pairs:
                # Create a list of all token addresses
                token_addresses = set()
                for pair in pairs:
                    token_addresses.add(pair['token0'])
                    token_addresses.add(pair['token1'])

                    # Save pair to database
                    sql = """
                    INSERT OR REPLACE INTO chart_pairs (id, token0, token1)
                    VALUES (?, ?, ?)
                    """
                    await self.exec_sql(sql, pair["id"], pair["token0"], pair["token1"])

                self.log.info(f"Loaded {len(pairs)} pairs on startup")

                # Then load token symbols
                if token_addresses:
                    token_symbols = await self.fetch_token_symbols_from_graphql(list(token_addresses))
                    for address, symbol in token_symbols.items():
                        sql = """
                        INSERT OR REPLACE INTO chart_tokens (address, symbol, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        """
                        await self.exec_sql(sql, address, symbol)

                    self.log.info(f"Loaded {len(token_symbols)} token symbols on startup")

            # We don't load trades on startup as there could be too many
            # They'll be loaded when needed or by the background job

        except Exception as e:
            self.log.error(f"Error loading initial data: {e}")
            await self.notify(e)

    async def fetch_pairs_from_graphql(self):
        """Fetch pairs directly from GraphQL, without database interaction"""
        pairs_query = await self.get_resource("get_pairs.gql")
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

    async def fetch_token_symbols_from_graphql(self, token_contracts):
        """Fetch token symbols directly from GraphQL, without database interaction"""
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

    async def fetch_swap_events_from_graphql(self, pair_id, created_after=None):
        """Fetch all new swap events from GraphQL using cursor pagination."""
        query = await self.get_resource("get_swap_events.gql")
        created_after = created_after or "1970-01-01T00:00:00Z"
        after = None
        events = []

        while True:
            result = await self.fetch_graphql(query, {
                "pairId": pair_id,
                "createdAfter": created_after,
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

    async def update_trades_job(self, context: CallbackContext):
        """Background job to update trades for all known pairs"""
        try:
            # Get all known pairs from the database
            sql = "SELECT id FROM chart_pairs"
            result = await self.exec_sql(sql)

            if not result["success"] or not result["data"]:
                return

            for pair_data in result["data"]:
                pair_id = pair_data[0]
                await self.fetch_and_store_new_trades(pair_id)

        except Exception as e:
            self.log.error(f"Error in update_trades_job: {e}")
            await self.notify(e)

    async def update_pairs_job(self, context: CallbackContext):
        """Background job to update trading pairs"""
        try:
            # Fetch pairs from GraphQL
            pairs = await self.fetch_pairs_from_graphql()

            if not pairs:
                return

            # Update database
            for pair in pairs:
                sql = """
                INSERT OR REPLACE INTO chart_pairs (id, token0, token1)
                VALUES (?, ?, ?)
                """
                await self.exec_sql(sql, pair["id"], pair["token0"], pair["token1"])

            self.log.info(f"Updated {len(pairs)} trading pairs")

        except Exception as e:
            self.log.error(f"Error in update_pairs_job: {e}")
            await self.notify(e)

    async def update_tokens_job(self, context: CallbackContext):
        """Background job to update token symbols"""
        try:
            # Get all token addresses from the pairs table
            sql = """
                  SELECT DISTINCT token0 \
                  FROM chart_pairs
                  UNION
                  SELECT DISTINCT token1 \
                  FROM chart_pairs \
                  """
            result = await self.exec_sql(sql)

            if not result["success"] or not result["data"]:
                return

            # Collect token addresses
            token_addresses = [row[0] for row in result["data"]]

            # Fetch token symbols
            token_symbols = await self.fetch_token_symbols_from_graphql(token_addresses)

            # Update database
            for address, symbol in token_symbols.items():
                sql = """
                INSERT OR REPLACE INTO chart_tokens (address, symbol, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """
                await self.exec_sql(sql, address, symbol)

            self.log.info(f"Updated {len(token_symbols)} token symbols")

        except Exception as e:
            self.log.error(f"Error in update_tokens_job: {e}")
            await self.notify(e)

    async def update_trades_for_pair(self, pair_id):
        """Update trades for a specific pair"""
        try:
            # Get the latest trade timestamp for this pair
            sql = """
                  SELECT timestamp \
                  FROM chart_trades
                  WHERE pair_id = ?
                  ORDER BY timestamp DESC
                  LIMIT 1 \
                  """
            result = await self.exec_sql(sql, pair_id)

            last_timestamp = None
            if result["success"] and result["data"]:
                last_timestamp_str = result["data"][0][0]
                last_timestamp = datetime.fromisoformat(last_timestamp_str)

            # Fetch new trades from GraphQL
            events = await self.fetch_swap_events(pair_id)
            if not events:
                return

            # Process events into trades
            new_trades = []
            for edge in events:
                node = edge["node"]

                # Parse data
                swap_data = json.loads(node["data"]) if isinstance(node["data"], str) else node["data"]

                # Parse timestamp
                timestamp_str = node["created"]
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                # Skip if we already have this trade
                if last_timestamp and timestamp <= last_timestamp:
                    continue

                amount0_in = float(swap_data.get("amount0In", 0) or 0)
                amount0_out = float(swap_data.get("amount0Out", 0) or 0)
                amount1_in = float(swap_data.get("amount1In", 0) or 0)
                amount1_out = float(swap_data.get("amount1Out", 0) or 0)

                # Calculate price (this is simplified, the actual price calculation
                # depends on which token is the base token)
                price = 0
                volume = 0

                if amount0_out > 0 and amount1_in > 0:
                    price = amount1_in / amount0_out
                    volume = amount0_out
                elif amount0_in > 0 and amount1_out > 0:
                    price = amount1_out / amount0_in
                    volume = amount0_in

                # Create a unique ID for this trade
                trade_id = f"{pair_id}_{timestamp_str}"

                new_trades.append({
                    "id": trade_id,
                    "pair_id": pair_id,
                    "timestamp": timestamp,
                    "price": price,
                    "volume": volume,
                    "amount0_in": amount0_in,
                    "amount0_out": amount0_out,
                    "amount1_in": amount1_in,
                    "amount1_out": amount1_out
                })

            # Save new trades to database
            if new_trades:
                for trade in new_trades:
                    sql = """
                          INSERT OR IGNORE INTO chart_trades
                          (id, pair_id, timestamp, price, volume, amount0_in, amount0_out, amount1_in, amount1_out)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
                          """
                    await self.exec_sql(
                        sql,
                        trade["id"],
                        trade["pair_id"],
                        trade["timestamp"].isoformat(),
                        trade["price"],
                        trade["volume"],
                        trade["amount0_in"],
                        trade["amount0_out"],
                        trade["amount1_in"],
                        trade["amount1_out"]
                    )

                self.log.info(f"Added {len(new_trades)} new trades for pair {pair_id}")

        except Exception as e:
            self.log.error(f"Error updating trades for pair {pair_id}: {e}")
            await self.notify(e)

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

    @TGBFPlugin.logging()
    @TGBFPlugin.send_typing()
    @TGBFPlugin.blacklist()
    async def chart_callback(self, update: Update, context: CallbackContext):
        """Handle the candlestick chart command"""
        # Don't deal with edited messages
        if not update.message:
            return

        # Set default values when no arguments provided
        if not context.args:
            context.args = ["XIAN-XUSDC", "72h"]
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
                    context.args = [f"{arg.upper()}-XIAN", "72h"]
                else:
                    # It's already a pair, add default timeframe
                    context.args = [arg, "72h"]
        elif len(context.args) == 2:
            # If first argument is just a ticker (no dash), construct ticker-XIAN pair
            if '-' not in context.args[0]:
                context.args[0] = f"{context.args[0].upper()}-XIAN"
        else:
            await update.message.reply_text(
                await self.get_info()
            )
            return

        # Parse trading pair symbol (e.g., "XIANUSDT")
        pair_str = context.args[0].upper()

        # Split the pair string (e.g., "XIANUSDT" -> "XIAN", "USDT")
        if "-" in pair_str:
            base_symbol, quote_symbol = pair_str.split("-")
        else:
            base_symbol = pair_str
            quote_symbol = "XUSDC"  # Default

        # Default to 1h candles for last 100 hours if no timeframe specified
        interval_minutes = 60  # 1 hour
        limit = 100  # 100 intervals

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

        try:
            # Find the pair by symbols
            pair = await self.find_pair_by_symbols(base_symbol, quote_symbol)

            if not pair:
                await update.message.reply_text(f"{con.ERROR} Trading pair {base_symbol}-{quote_symbol} not found")
                return

            # Determine if base token is token0 or token1
            base_is_token0 = pair.get('token0_symbol', '').upper() == base_symbol.upper()

            # Get swap events
            events = await self.fetch_swap_events(pair['id'])

            if not events:
                await update.message.reply_text(f"{con.ERROR} No swap events found for {base_symbol}-{quote_symbol}")
                return

            # Process into candles
            candles = self.process_swap_events(events, interval_minutes, limit, base_is_token0)

            if not candles:
                timeframe_str = context.args[1] if len(context.args) > 1 else '72h'
                await update.message.reply_text(
                    f"{con.INFO} No trades found for {base_symbol}-{quote_symbol} "
                    f"in the last {timeframe_str}")
                return

            # Create DataFrame for Plotly
            df = pd.DataFrame(candles)

            # Current price for price line
            current_price = df['close'].values[-1]

            # Calculate min and max price for better y-axis scaling (add 5% padding)
            y_min = min(df['low']) * 0.95
            y_max = max(df['high']) * 1.05

            # Define colors
            up_color = 'rgb(0, 171, 107)'  # Green for bullish candles
            down_color = 'rgb(255, 73, 73)'  # Red for bearish candles
            bg_color = 'rgb(25, 25, 40)'  # Dark blue background
            grid_color = 'rgba(255, 255, 255, 0.1)'  # Subtle grid
            text_color = 'rgba(255, 255, 255, 0.9)'  # White text

            tf = context.args[1] if len(context.args) > 1 else '72h'

            # Create a new figure with subplots (price and volume)
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.8, 0.2],
                subplot_titles=(f"{base_symbol}-{quote_symbol} {tf}", "Volume")
            )

            # Add candlestick trace
            fig.add_trace(
                go.Candlestick(
                    x=df['time'],
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close'],
                    increasing=dict(line=dict(color=up_color), fillcolor=up_color),
                    decreasing=dict(line=dict(color=down_color), fillcolor=down_color),
                    name='Price'
                ),
                row=1, col=1
            )

            # Calculate and add volume bars
            colors = [up_color if df['close'][i] >= df['open'][i] else down_color for i in range(len(df))]
            fig.add_trace(
                go.Bar(
                    x=df['time'],
                    y=df['volume'],
                    marker_color=colors,
                    name='Volume',
                    opacity=0.7
                ),
                row=2, col=1
            )

            # Add moving averages if we have enough data
            if len(df) >= 20:
                df['MA20'] = df['close'].rolling(window=20).mean()
                fig.add_trace(
                    go.Scatter(
                        x=df['time'],
                        y=df['MA20'],
                        line=dict(color='rgba(255, 207, 0, 0.7)', width=2),
                        name='20-period MA'
                    ),
                    row=1, col=1
                )

            if len(df) >= 50:
                df['MA50'] = df['close'].rolling(window=50).mean()
                fig.add_trace(
                    go.Scatter(
                        x=df['time'],
                        y=df['MA50'],
                        line=dict(color='rgba(144, 238, 144, 0.7)', width=2),
                        name='50-period MA'
                    ),
                    row=1, col=1
                )

            # Add current price line
            fig.add_shape(
                type="line",
                x0=df['time'].min(),
                x1=df['time'].max(),
                y0=current_price,
                y1=current_price,
                line=dict(color='rgba(102, 204, 255, 0.8)', width=2, dash="dot"),
                row=1, col=1
            )

            # Update layout with modern styling
            fig.update_layout(
                paper_bgcolor=bg_color,
                plot_bgcolor=bg_color,
                font=dict(
                    family="Arial, sans-serif",
                    size=13,
                    color=text_color
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.05,
                    xanchor="center",
                    x=0.5,
                    bgcolor="rgba(0,0,0,0.3)",
                    bordercolor="rgba(255,255,255,0.2)",
                    font=dict(color=text_color)
                ),
                margin=dict(l=60, r=60, t=80, b=60),
                height=700,  # Taller chart for better visibility
                hovermode="x unified",
                xaxis_rangeslider_visible=False
            )

            # Style the main price chart
            fig.update_xaxes(
                row=1, col=1,
                gridcolor=grid_color,
                zerolinecolor=grid_color,
                showspikes=True,
                spikethickness=1,
                spikedash="solid",
                spikecolor="rgba(255,255,255,0.4)",
                spikemode="across"
            )

            fig.update_yaxes(
                row=1, col=1,
                gridcolor=grid_color,
                zerolinecolor=grid_color,
                title=f"Price ({quote_symbol})",
                titlefont=dict(size=13),
                showspikes=True,
                spikethickness=1,
                spikedash="solid",
                spikecolor="rgba(255,255,255,0.4)",
                spikemode="across",
                range=[y_min, y_max]  # Set y-axis range with padding
            )

            # Style the volume chart
            fig.update_xaxes(
                row=2, col=1,
                gridcolor=grid_color,
                zerolinecolor=grid_color
            )

            fig.update_yaxes(
                row=2, col=1,
                gridcolor=grid_color,
                zerolinecolor=grid_color,
                title=f"Volume ({base_symbol})",
                titlefont=dict(size=13)
            )

            # Calculate 24h volume (if we have enough data)
            volume_24h = self.calculate_24h_volume_from_trades(events, base_is_token0)

            caption = (
                f"<code>Last Price: {current_price:,.8g} {quote_symbol}</code>\n"
                f"<code>24h Volume: {volume_24h:,.2f} {base_symbol}</code>"
            )

            # Send chart as photo with higher resolution
            await update.message.reply_photo(
                photo=io.BufferedReader(io.BytesIO(pio.to_image(fig, format='png', scale=2))),
                caption=caption
            )

        except Exception as e:
            #await update.message.reply_text(f"{con.ERROR} Error creating chart: {e}")
            self.log.error(f"Chart error: {e}")
            #await self.notify(e)

    async def fetch_pairs(self):
        """Fetch pairs from database, or from GraphQL if not available"""
        # Try to get from database first
        sql = "SELECT id, token0, token1 FROM chart_pairs"
        result = await self.exec_sql(sql)

        if result["success"] and result["data"]:
            pairs = []
            for row in result["data"]:
                pairs.append({
                    'id': row[0],
                    'token0': row[1],
                    'token1': row[2]
                })
            return pairs

        # If not in database, fetch from GraphQL
        pairs = await self.fetch_pairs_from_graphql()

        # Save to database
        if pairs:
            for pair in pairs:
                sql = """
                INSERT OR REPLACE INTO chart_pairs (id, token0, token1)
                VALUES (?, ?, ?)
                """
                await self.exec_sql(sql, pair["id"], pair["token0"], pair["token1"])

        return pairs

    async def fetch_token_symbols(self, token_contracts):
        """Fetch token symbols from database, or from GraphQL if not available"""
        if not token_contracts:
            return {}

        token_symbols = {}
        missing_tokens = []

        # Try to get from database first
        for token in token_contracts:
            sql = "SELECT symbol FROM chart_tokens WHERE address = ?"
            result = await self.exec_sql(sql, token)

            if result["success"] and result["data"]:
                token_symbols[token] = result["data"][0][0]
            else:
                missing_tokens.append(token)

        # If any tokens missing from database, fetch from GraphQL
        if missing_tokens:
            new_symbols = await self.fetch_token_symbols_from_graphql(missing_tokens)

            # Save to database
            for token, symbol in new_symbols.items():
                sql = """
                INSERT OR REPLACE INTO chart_tokens (address, symbol, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """
                await self.exec_sql(sql, token, symbol)

            # Add to result
            token_symbols.update(new_symbols)

        return token_symbols

    async def find_pair_by_symbols(self, base_symbol, quote_symbol='XUSDC'):
        """Find a pair by base and quote symbols, using database if available"""
        # Try to find the pair in the database first
        sql = """
              SELECT p.id, p.token0, p.token1, t0.symbol as token0_symbol, t1.symbol as token1_symbol
              FROM chart_pairs p
                       JOIN chart_tokens t0 ON p.token0 = t0.address
                       JOIN chart_tokens t1 ON p.token1 = t1.address
              WHERE (t0.symbol = ? AND t1.symbol = ?) \
                 OR (t0.symbol = ? AND t1.symbol = ?) \
              """
        result = await self.exec_sql(
            sql,
            base_symbol.upper(), quote_symbol.upper(),
            quote_symbol.upper(), base_symbol.upper()
        )

        if result["success"] and result["data"]:
            pair_data = result["data"][0]
            pair = {
                'id': pair_data[0],
                'token0': pair_data[1],
                'token1': pair_data[2],
                'token0_symbol': pair_data[3],
                'token1_symbol': pair_data[4]
            }
            self.log.debug(f"Found pair in database: {pair}")
            return pair

        # If not found in database, fetch from GraphQL
        self.log.debug(f"Pair {base_symbol}-{quote_symbol} not found in database, fetching from GraphQL")
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

                # Save to database
                await self.exec_sql(
                    "INSERT OR REPLACE INTO chart_pairs (id, token0, token1) VALUES (?, ?, ?)",
                    pair['id'], pair['token0'], pair['token1']
                )

                # Save token symbols
                await self.exec_sql(
                    "INSERT OR REPLACE INTO chart_tokens (address, symbol, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    pair['token0'], pair['token0_symbol']
                )
                await self.exec_sql(
                    "INSERT OR REPLACE INTO chart_tokens (address, symbol, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    pair['token1'], pair['token1_symbol']
                )

                return pair

        return None

    async def fetch_swap_events(self, pair_id):
        """Fetch swap events from database, fetching new events from GraphQL if needed"""
        # First check if we need to update from GraphQL
        await self.fetch_and_store_new_trades(pair_id)

        # Get all trades from database
        sql = """
              SELECT timestamp, data
              FROM chart_trades
              WHERE pair_id = ?
              ORDER BY timestamp DESC \
              """
        result = await self.exec_sql(sql, pair_id)

        edges = []
        if result["success"] and result["data"]:
            for row in result["data"]:
                timestamp = row[0]
                trade_data = json.loads(row[1])

                # Create a node structure similar to what comes from GraphQL
                node = {
                    "dataIndexed": json.dumps({"pair": pair_id}),
                    "data": json.dumps(trade_data),
                    "created": timestamp
                }

                edges.append({"node": node})

        return edges

    async def fetch_and_store_new_trades(self, pair_id):
        """Fetch new trades from GraphQL and store them in the database"""
        try:
            latest_result = await self.exec_sql(
                """
                SELECT timestamp
                FROM chart_trades
                WHERE pair_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                pair_id,
            )
            if not latest_result["success"]:
                raise RuntimeError(f"Unable to read the latest trade for pair {pair_id}")

            last_timestamp = None
            if latest_result["data"]:
                last_timestamp = datetime.fromisoformat(
                    latest_result["data"][0][0].replace("Z", "+00:00")
                )

            # Replaying a bounded window catches events that become visible out
            # of timestamp order. During the one-time ID migration, replay from
            # the earliest legacy timestamp so every old row can be reconciled.
            legacy_min_result = await self.exec_sql(
                """
                SELECT MIN(timestamp)
                FROM chart_trades
                WHERE id GLOB ?
                """,
                f"{pair_id}_*",
            )
            if not legacy_min_result["success"]:
                raise RuntimeError(f"Unable to inspect legacy trades for pair {pair_id}")

            earliest_legacy = None
            if legacy_min_result["data"] and legacy_min_result["data"][0][0]:
                earliest_legacy = legacy_min_result["data"][0][0]

            if earliest_legacy:
                created_after = earliest_legacy
            elif last_timestamp:
                created_after = (last_timestamp - timedelta(hours=24)).isoformat()
            else:
                created_after = "1970-01-01T00:00:00Z"

            stored_result = await self.exec_sql(
                """
                SELECT id, data, timestamp
                FROM chart_trades
                WHERE pair_id = ? AND timestamp >= ?
                """,
                pair_id,
                created_after,
            )
            if not stored_result["success"]:
                raise RuntimeError(f"Unable to read stored trades for pair {pair_id}")

            known_trade_ids = set()
            legacy_trades = []
            for row in stored_result["data"] or []:
                stored_id = str(row[0])
                known_trade_ids.add(stored_id)
                if stored_id.startswith(f"{pair_id}_"):
                    legacy_trades.append({
                        "id": stored_id,
                        "data": json.loads(row[1]),
                        "timestamp": datetime.fromisoformat(row[2].replace("Z", "+00:00")),
                    })

            events = await self.fetch_swap_events_from_graphql(
                pair_id,
                created_after=created_after,
            )
            if not events:
                return

            # Process and store new events
            new_trades = 0
            for edge in events:
                node = edge["node"]

                # Parse timestamp
                timestamp_str = node["created"]
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                # Parse data
                data = node["data"]
                if isinstance(data, str):
                    data = json.loads(data)

                if node.get("id") is None:
                    raise RuntimeError("GraphQL swap event is missing its stable ID")
                trade_id = str(node["id"])
                legacy_match = next(
                    (
                        trade for trade in legacy_trades
                        if trade["timestamp"] == timestamp and trade["data"] == data
                    ),
                    None,
                )

                if trade_id in known_trade_ids:
                    if legacy_match:
                        delete_result = await self.exec_sql(
                            "DELETE FROM chart_trades WHERE id = ?",
                            legacy_match["id"],
                        )
                        if delete_result["success"]:
                            known_trade_ids.discard(legacy_match["id"])
                            legacy_trades.remove(legacy_match)
                    continue

                if legacy_match:
                    update_result = await self.exec_sql(
                        "UPDATE chart_trades SET id = ? WHERE id = ?",
                        trade_id,
                        legacy_match["id"],
                    )
                    if update_result["success"]:
                        known_trade_ids.discard(legacy_match["id"])
                        known_trade_ids.add(trade_id)
                        legacy_trades.remove(legacy_match)
                        continue

                # Save to database
                sql = """
                      INSERT OR IGNORE INTO chart_trades
                          (id, pair_id, timestamp, data)
                      VALUES (?, ?, ?, ?) \
                      """
                result = await self.exec_sql(
                    sql,
                    trade_id,
                    pair_id,
                    timestamp.isoformat(),
                    json.dumps(data)
                )

                if not result["success"]:
                    raise RuntimeError(
                        f"Unable to store GraphQL event {trade_id} for pair {pair_id}"
                    )

                known_trade_ids.add(trade_id)
                new_trades += 1
                if legacy_match:
                    delete_result = await self.exec_sql(
                        "DELETE FROM chart_trades WHERE id = ?",
                        legacy_match["id"],
                    )
                    if delete_result["success"]:
                        known_trade_ids.discard(legacy_match["id"])
                        legacy_trades.remove(legacy_match)

            if new_trades > 0:
                self.log.info(f"Added {new_trades} new trades for pair {pair_id}")

        except Exception as e:
            self.log.error(f"Error fetching new trades for pair {pair_id}: {e}")
            await self.notify(e)

    def process_swap_events(self, events, interval_minutes, limit, base_is_token0=True):
        """Convert swap events to candlestick data"""
        if not events:
            return []

        # Hardcode the first trade timestamp
        FIRST_TRADE_TIMESTAMP = datetime.fromisoformat("2025-03-27T17:29:38".replace('Z', '+00:00'))

        # Extract trades
        trades = []
        for edge in events:
            node = edge['node']

            # Parse data
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
        requested_start_time = current_time - timedelta(minutes=interval_minutes * limit)

        # Ensure we don't go before the first trade timestamp
        start_time = max(requested_start_time, FIRST_TRADE_TIMESTAMP)

        # Round to interval boundaries (as in web implementation)
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
