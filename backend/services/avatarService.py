import os
import logging
from typing import Optional
from PIL import Image, ImageDraw, ImageFont
import numpy as np

logger = logging.getLogger(__name__)

USER_AVATAR_DIRS = [
    "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/resources/images/users",
    "/home/jcolinm/.build-cache/atalaia-frontend/target/classes/META-INF/resources/resources/images/users"
]

def generate_chess_avatar(role_name: str, output_path: str, size: int = 256) -> str:
    """
    Genera o copia el avatar de ajedrez representativo de alta resolución:
      - Administrador: Alfil de ajedrez geométrico (Bishop)
      - Analista:      Torre de ajedrez geométrica (Rook)
      - Trader:        Caballo de ajedrez geométrico (Knight)
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    role_lower = (role_name or "").lower().strip()
    if "admin" in role_lower:
        template_name = "alfil.png"
    elif "analis" in role_lower:
        template_name = "torre.png"
    else:
        template_name = "caballo.png"

    template_path = os.path.join(USER_AVATAR_DIRS[0], template_name)
    if os.path.exists(template_path) and os.path.abspath(template_path) != os.path.abspath(output_path):
        import shutil
        shutil.copyfile(template_path, output_path)
        return output_path

    scale = 4
    dim = size * scale
    im = Image.new("RGBA", (dim, dim), (0, 0, 0, 0))
    draw = ImageDraw.Draw(im)

    if "admin" in role_lower:
        glyph = "\u265D"  # Alfil / Bishop ♝
        bg_color = (30, 41, 59, 255)       # Slate Navy #1e293b
        border_color = (217, 119, 6, 255)  # Gold Amber #d97706
        piece_color = (255, 255, 255, 255)
    elif "analis" in role_lower:
        glyph = "\u265C"  # Torre / Rook ♜
        bg_color = (15, 118, 110, 255)     # Deep Teal #0f766e
        border_color = (45, 212, 191, 255) # Mint Teal #2dd4bf
        piece_color = (255, 255, 255, 255)
    else:  # Trader o por defecto
        glyph = "\u265E"  # Caballo / Knight ♞
        bg_color = (180, 83, 9, 255)       # Warm Bronze #b45309
        border_color = (251, 191, 36, 255) # Bright Amber #fbbf24
        piece_color = (255, 255, 255, 255)

    margin = 3 * scale
    border_w = 4 * scale
    circle_d = dim - 2 * margin
    r_circle = (circle_d - 2 * border_w) / 2
    # El icono casi toca el círculo (distancia de ~3px a escala 1x)
    target_reach = r_circle - (3 * scale)

    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if not os.path.exists(font_path):
        font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

    # Auto-escalado iterativo para que el icono ocupe el máximo espacio posible sin salirse del círculo
    fs = int(dim * 0.85)
    for _ in range(5):
        im_test = Image.new("RGBA", (dim, dim), (0, 0, 0, 0))
        d_test = ImageDraw.Draw(im_test)
        font = ImageFont.truetype(font_path, fs)
        bbox = d_test.textbbox((0, 0), glyph, font=font)
        w_t, h_t = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x_t = (dim - w_t) / 2 - bbox[0]
        y_t = (dim - h_t) / 2 - bbox[1]
        d_test.text((x_t, y_t), glyph, font=font, fill=(255, 255, 255, 255))

        arr = np.array(im_test)
        cx, cy = dim / 2, dim / 2
        white_pts = np.where((arr[:, :, 0] == 255) & (arr[:, :, 1] == 255) & (arr[:, :, 2] == 255))
        if len(white_pts[0]) == 0:
            break
        dists = np.sqrt((white_pts[1] - cx) ** 2 + (white_pts[0] - cy) ** 2)
        reach = dists.max()
        if abs(reach - target_reach) < 2.0:
            break
        scale_ratio = target_reach / reach
        fs = int(fs * scale_ratio)

    # Dibujar círculo de fondo y borde
    draw.ellipse([margin, margin, dim - margin, dim - margin], fill=bg_color, outline=border_color, width=border_w)

    # Dibujar glifo de ajedrez centrado
    font = ImageFont.truetype(font_path, fs)
    bbox = draw.textbbox((0, 0), glyph, font=font)
    w_t, h_t = bbox[2] - bbox[0], bbox[3] - bbox[1]
    x_t = (dim - w_t) / 2 - bbox[0]
    y_t = (dim - h_t) / 2 - bbox[1]
    draw.text((x_t, y_t), glyph, font=font, fill=piece_color)

    # Redimensionar con Lanczos para acabado súper nítido
    im_final = im.resize((size, size), Image.Resampling.LANCZOS)
    im_final.save(output_path, "PNG")
    return output_path

def ensure_user_avatar(username: str, role_name: str, force: bool = False) -> None:
    """
    Garantiza que exista el archivo <username>.png en los directorios de recursos estáticos.
    Si ya existe y force=False, preserva la foto existente del usuario.
    Si no existe o force=True, genera el avatar por defecto con la pieza de ajedrez según su rol.
    """
    clean_user = (username or "").strip().lower()
    if not clean_user:
        return

    for udir in USER_AVATAR_DIRS:
        target_path = os.path.join(udir, f"{clean_user}.png")
        if force or not os.path.exists(target_path):
            try:
                generate_chess_avatar(role_name, target_path)
                logger.info(f"♟️ [AVATAR] Generado avatar para usuario '{clean_user}' ({role_name}) -> {target_path}")
            except Exception as e:
                logger.error(f"❌ [AVATAR] Error generando avatar para '{clean_user}': {e}")

def ensure_all_user_avatars(db_session=None, force: bool = False) -> int:
    """
    Escanea todos los usuarios registrados en MySQL y asegura que cada uno tenga su archivo <username>.png.
    """
    from sqlalchemy import text
    from backend.database.models import SessionLocal

    session = db_session or SessionLocal()
    created_count = 0
    try:
        rows = session.execute(text("""
            SELECT u.username, COALESCE(r.nameRole, 'Trader') AS roleName
            FROM usuario u
            LEFT JOIN role r ON r.idRole = u.idRole
            WHERE u.status = 1
        """)).fetchall()

        for row in rows:
            uname = str(row[0]).strip().lower()
            rname = str(row[1]).strip()
            needs_gen = force or any(not os.path.exists(os.path.join(d, f"{uname}.png")) for d in USER_AVATAR_DIRS)
            if needs_gen:
                ensure_user_avatar(uname, rname, force=force)
                created_count += 1
        logger.info(f"✅ [AVATARS] Auditoría de fotos finalizada. {len(rows)} usuarios verificados, {created_count} actualizados.")
    except Exception as e:
        logger.error(f"❌ [AVATARS] Error verificando avatares de usuarios: {e}")
    finally:
        if not db_session:
            session.close()
    return created_count
