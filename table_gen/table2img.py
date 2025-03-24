import os
import random
import string
import pandas as pd
import dataframe_image as dfi
from PIL import Image, ImageDraw, ImageFont
import subprocess
import tempfile

table_error_count = 0


def generate_table_image(table_data, table_name, output_dir):
    """
    Generates an image from a JSON table with the table name overlaid using pandas and dataframe_image.
    Randomly selects a table style from a diverse set of styles.
    Args:
        table_data: JSON data representing the table.
        table_name: The name of the table.
        output_dir: Directory to save the generated image.
    Returns:
        A tuple containing the image ID and the path to the saved image.
    """
    global table_error_count

    df = None
    if "table_columns" in table_data and "table_content" in table_data:
        df = pd.DataFrame(
            table_data["table_content"], columns=table_data["table_columns"]
        )
    else:
        df = pd.DataFrame(table_data)

    num_rows, num_cols = df.shape

    # Generate a 5-character random image ID
    image_id = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=5)
    ).capitalize()
    image_filename = f"TableImg_{image_id}_{num_rows}.png"
    image_path = os.path.join(output_dir, image_filename)

    # Define a function for alternating row colors
    def alternating_row_colors(df, color1="#f2f2f2", color2="white"):
        return [
            f"background-color: {color1}" if i % 2 else f"background-color: {color2}"
            for i in range(len(df))
        ]

    # Define a variety of table styles
    table_styles = [
        # Style 1: Green header with bold font, alternating row colors
        lambda df: df.style.apply(alternating_row_colors, axis=0).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#4CAF50"),
                        ("color", "white"),
                        ("font-weight", "bold"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("border", "1px solid black"), ("text-align", "center")],
                },
                {"selector": "table", "props": [("border-collapse", "collapse")]},
            ]
        ),
        # Style 2: Minimalist style with light grey background
        lambda df: df.style.set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#E0E0E0"),
                        ("color", "black"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("text-align", "center"), ("background-color", "white")],
                },
            ]
        ),
        # Style 3: Dark header with white text, bold rows
        lambda df: df.style.apply(
            alternating_row_colors, axis=0, color1="#E6E6E6", color2="white"
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#333"),
                        ("color", "white"),
                        ("text-align", "center"),
                        ("border", "1px solid white"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("border", "1px solid #999"), ("text-align", "center")],
                },
            ]
        ),
        # Style 4: Blue header with bold white text and gridlines
        lambda df: df.style.apply(
            alternating_row_colors, axis=0, color1="#F8F8F8", color2="#FFFFFF"
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#1E88E5"),
                        ("color", "white"),
                        ("font-weight", "bold"),
                        ("text-align", "center"),
                        ("border", "2px solid #000"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("border", "1px solid #888"), ("text-align", "center")],
                },
            ]
        ),
        # Style 5: Modern white and grey with no borders
        lambda df: df.style.set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#F0F0F0"),
                        ("font-size", "12pt"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("background-color", "#FAFAFA"),
                        ("text-align", "center"),
                    ],
                },
            ]
        ),
        # Style 6: Orange header with rounded borders
        lambda df: df.style.set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#FF7043"),
                        ("color", "white"),
                        ("border", "1px solid #FF5722"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("text-align", "center"),
                        ("border", "1px solid #FFCCBC"),
                    ],
                },
            ]
        ),
        # Style 7: Red header with bold font, striped rows
        lambda df: df.style.apply(
            alternating_row_colors, axis=0, color1="#FFEBEE", color2="#FFFFFF"
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#B71C1C"),
                        ("color", "white"),
                        ("font-weight", "bold"),
                        ("text-align", "center"),
                    ],
                },
                {"selector": "td", "props": [("text-align", "center")]},
            ]
        ),
        # Style 8: Elegant black-and-white style
        lambda df: df.style.set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "black"),
                        ("color", "white"),
                        ("text-align", "center"),
                        ("border", "1px solid white"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("background-color", "white"),
                        ("color", "black"),
                        ("border", "1px solid black"),
                        ("text-align", "center"),
                    ],
                },
            ]
        ),
        # Style 9: Pastel blue theme with centered text
        lambda df: df.style.set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#BBDEFB"),
                        ("color", "#0D47A1"),
                        ("text-align", "center"),
                        ("font-weight", "bold"),
                    ],
                },
                {"selector": "td", "props": [("text-align", "center")]},
            ]
        ),
        # Style 10: Vibrant yellow header with alternating bold text rows
        lambda df: df.style.apply(
            alternating_row_colors, axis=0, color1="#FFFDE7", color2="#FFF9C4"
        ).set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#FBC02D"),
                        ("color", "black"),
                        ("font-weight", "bold"),
                        ("text-align", "center"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("border", "1px solid #F57F17"),
                        ("text-align", "center"),
                    ],
                },
            ]
        ),
    ]

    # Randomly select a style
    selected_style = random.choice(table_styles)

    # Apply the selected style to the DataFrame
    styled_df = selected_style(df).hide(axis="index")

    # Define possible conversions with custom probabilities
    conversion_options = [("selenium", 0.7), ("matplotlib", 0.3)]

    # Select one based on the given probabilities
    table_conversion = random.choices(
        [option[0] for option in conversion_options],
        weights=[option[1] for option in conversion_options],
        k=1,
    )[0]

    # Use the library's default export for other conversions (html2image, matplotlib)
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Use a temporary file within the temporary directory
            temp_image_path = os.path.join(temp_dir, f"temp_table_{image_id}.png")

            # Use the library's default export for other conversions (html2image, matplotlib)
            dfi.export(
                styled_df,
                temp_image_path,
                table_conversion=table_conversion,
                max_rows=-1,
                max_cols=-1,
            )
            subprocess.run(["cp", temp_image_path, image_path])

        except Exception as e:
            table_error_count += 1  # Use the global counter
            print(
                f"Error exporting table to image: {e}, Total errors: {table_error_count}"
            )
            return None, None  # Or handle the error as appropriate

    # Add table name above the image
    try:
        table_img = Image.open(image_path)
        table_width, table_height = table_img.size

        font = ImageFont.load_default()
        padding = 20
        table_name_upper = table_name.upper()

        draw = ImageDraw.Draw(table_img)
        text_bbox = draw.textbbox((0, 0), table_name_upper, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]

        new_img_width = table_width
        new_img_height = table_height + text_height + (2 * padding)
        new_img = Image.new("RGB", (new_img_width, new_img_height), "white")
        new_draw = ImageDraw.Draw(new_img)

        table_name_x = (table_width - text_width) // 2
        table_name_y = padding // 2

        new_draw.rectangle(
            [0, 0, new_img_width, text_height + 2 * padding], fill="white"
        )
        new_draw.text(
            (table_name_x, table_name_y), table_name_upper, font=font, fill="black"
        )

        new_img.paste(table_img, (0, text_height + 2 * padding))
        new_img.save(image_path)

    except Exception as e:
        print(f"Error adding table name to image: {e}")

    return image_filename, image_path
