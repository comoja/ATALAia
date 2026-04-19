import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- DATOS SINTÉTICOS PARA EJEMPLO ---

# 1. Datos para FVG ALCISTA (Configuración LARGO)
data_long = {
    'open':  [100, 102, 110, 108, 107, 108, 112, 115],
    'high':  [105, 115, 112, 110, 109, 113, 118, 120],
    'low':   [98,  101, 109, 106, 105, 108, 111, 114],
    'close': [102, 110, 108, 107, 108, 112, 116, 119]
}
df_long = pd.DataFrame(data_long)
# FVG detectado entre Vela 0(High) y Vela 2(Low)
fvg_l_top = df_long.loc[2, 'low']    # 109
fvg_l_bot = df_long.loc[0, 'high']   # 105

# 2. Datos para FVG BAJISTA (Configuración CORTO)
data_short = {
    'open':  [200, 198, 190, 192, 193, 192, 188, 185],
    'high':  [202, 199, 191, 194, 195, 192, 189, 186],
    'low':   [195, 185, 188, 190, 191, 187, 182, 180],
    'close': [198, 190, 192, 193, 192, 188, 184, 181]
}
df_short = pd.DataFrame(data_short)
# FVG detectado entre Vela 0(Low) y Vela 2(High)
fvg_s_top = df_short.loc[0, 'low']    # 195
fvg_s_bot = df_short.loc[2, 'high']   # 191


# --- FUNCIÓN DE GRAFICADO ---
def plot_fvg_example(df, fvg_top, fvg_bot, fvg_type="Alcista"):
    
    color_fvg = "rgba(0, 50, 255, 0.2)" if fvg_type == "Alcista" else "rgba(255, 50, 0, 0.2)"
    line_color = "green" if fvg_type == "Alcista" else "red"
    title = f"Ejemplo de Entrada en FVG {fvg_type.upper()}"
    
    fig = go.Figure(data=[go.Candlestick(x=df.index,
                                         open=df['open'],
                                         high=df['high'],
                                         low=df['low'],
                                         close=df['close'],
                                         name='Precio')])

    # 1. Dibujar la Zona FVG (Recuadro Shaded)
    # Se extiende desde la vela 2 (confirmación) hasta el final del gráfico simulado
    fig.add_shape(type="rect",
                  x0=2, y0=fvg_bot, x1=len(df)-1, y1=fvg_top,
                  fillcolor=color_fvg, opacity=1, layer="below", line_width=0)

    # 2. Dibujar líneas de mechas límite
    fig.add_shape(type="line", x0=0, y0=fvg_bot, x1=2, y1=fvg_bot,
                  line=dict(color=line_color, width=2, dash="dash"), name="Límite 1")
    fig.add_shape(type="line", x0=0, y0=fvg_top, x1=2, y1=fvg_top,
                  line=dict(color=line_color, width=2, dash="dash"), name="Límite 3")

    # 3. Anotaciones
    text_entrada = "Zona de Compra (Largo)" if fvg_type == "Alcista" else "Zona de Venta (Corto)"
    
    fig.add_annotation(x=1, y=fvg_top, text="Vela 2 (Desplazamiento)", showarrow=True, arrowhead=1)
    fig.add_annotation(x=4, y=(fvg_top + fvg_bot)/2, text=text_entrada, showarrow=False, font=dict(color=line_color))
    fig.add_annotation(x=6, y=df.loc[6, 'close'], text="Reacción Institucional", showarrow=True, arrowhead=2)

    fig.update_layout(title=title, yaxis_title='Precio', xaxis_title='Índice de Vela',
                      xaxis_rangeslider_visible=False)
    fig.show()

# --- GENERAR LOS GRÁFICOS ---
plot_fvg_example(df_long, fvg_l_top, fvg_l_bot, fvg_type="Alcista")
plot_fvg_example(df_short, fvg_s_top, fvg_s_bot, fvg_type="Bajista")