# AlphaBetter

A tool based on machine learning that allows you to predict the results of matches and create
betting strategies.

## Usage

1.  Create database schema:
    ```console
    $ bin/alphabetter create-schema
    ```

2.  Collect historical match data from the web:
    ```console
    $ bin/alphabetter etl-championat-tournament
    ```

3.  Collect historical odds data from the web:
    ```console
    $ bin/alphabetter etl-line4bet-odds
    ```

4.  Calibrate and train your ML models via Jupyter:
    ```console
    $ jupyter-lab notebooks/model_selection.ipynb
    ```

5.  Collect fresh data from a bookmaker:
    ```console
    $ bin/alphabetter etl-fonbet-odds
    ```

6.  Get recommendations on the most profitable bets for the coming days:
    ```console
    $ bin/alphabetter recommend-bets --balance 10_000
    BM      Date    Time    League                Home team       Away team          M     $
    ------  ------  ------  --------------------  --------------  ---------------  ---  ----
    Fonbet  Feb 25  22:00   Eredivisie            АЗ Алкмар       Камбюр             1  1300
    Fonbet  Feb 25  22:45   Belgian Pro League    Вестерло        Юнион              2   720
    Fonbet  Feb 26  00:00   Süper Lig             Бешикташ        Истанбулспор       1   850
    Fonbet  Feb 28  20:30   Serie A               Кремонезе       Рома               2   660
    Fonbet  Feb 28  22:45   Serie A               Ювентус         Торино             1   600
    Fonbet  Mar 04  18:00   Premier League        Манчестер Сити  Ньюкасл Юнайтед    1   700
    Fonbet  Mar 05  16:30   Premier L...e Russia  Краснодар       Торпедо            1   630

    * BM - bookmaker
      M - recommended outcome to bet on
      $ - recommended bet money
    ```
