import streamlit as st
import requests
from PIL import Image
from io import BytesIO

API_URL = "http://127.0.0.1:8080"  # Backend FastAPI URL

BACKEND_URL = "https://damg-big-data-assignment01.onrender.com"

def pdf_to_markdown(file_bytes, file_name, method):
    files = {"file": (file_name, file_bytes, "application/pdf")}
    data = {"method": method}
    resp = requests.post(f"{API_URL}/extract/pdf/", files=files, data=data)
    resp.raise_for_status()
    return resp.json()


def website_to_markdown(url, method):
    data = {"url": url, "method": method}
    resp = requests.post(f"{API_URL}/extract/website/", data=data)
    resp.raise_for_status()
    return resp.json()


# Main App
def main():
    st.set_page_config(
        page_title="Markdown Generator",
        page_icon="📄",
        layout="wide",
    )

    st.title("📄 Markdown Generator")
    st.markdown(
        """
        **Easily extract text, images, and tables from PDFs or websites.**  
        Choose an input type, select an extraction method, and generate Markdown files with ease.
        """
    )

    # Sidebar
    st.sidebar.title("Navigation")
    choice = st.sidebar.radio("Select an input type", ["PDF to Markdown", "Website URL to Markdown"])

    # Custom CSS for better UI
    st.markdown(
        """
        <style>
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            border-radius: 8px;
            font-size: 16px;
            padding: 10px 20px;
        }
        .stButton>button:hover {
            background-color: #45a049;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if choice == "PDF to Markdown":
        st.subheader("Upload a PDF to Generate Markdown")

        # File Uploader
        uploaded_pdf = st.file_uploader("Upload a PDF file", type=["pdf"])

        # Extraction Method
        extraction_method = st.radio(
            "Extraction Method",
            ["Extract Using Open-Source Tool", "Extract Using Enterprise Tool"],
            horizontal=True,
        )

        # Generate Markdown Button
        if st.button("Generate Markdown"):
            if not uploaded_pdf:
                st.warning("🚨 Please upload a PDF file to proceed.")
                return

            with st.spinner("⏳ Processing your file..."):
                method_val = "open-source" if "Open-Source" in extraction_method else "enterprise"
                try:
                    response_data = pdf_to_markdown(uploaded_pdf.getvalue(), uploaded_pdf.name, method_val)
                    md_url = response_data["markdown_url"]
                    st.success("✅ Markdown generated successfully!")
                    st.write(f"📂 Markdown file uploaded to S3: [View Markdown]({md_url})")

                    # Preview and download images
                    if "image_urls" in response_data:  # Assuming the API returns a list of image URLs
                        st.subheader("📷 Extracted Images")
                        for idx, img_url in enumerate(response_data["image_urls"], start=1):
                            response = requests.get(img_url)
                            img = Image.open(BytesIO(response.content))
                            st.image(img, caption=f"Image {idx}", use_column_width=True)
                            st.download_button(
                                label=f"Download Image {idx}",
                                data=response.content,
                                file_name=f"image_{idx}.png",
                                mime="image/png",
                            )
                except Exception as e:
                    st.error(f"❌ An error occurred: {e}")

    elif choice == "Website URL to Markdown":
        st.subheader("Enter a Website URL to Generate Markdown")

        # Website URL Input
        url_input = st.text_input("Enter a Website URL", placeholder="https://example.com")

        # Extraction Method
        extraction_method = st.radio(
            "Extraction Method",
            ["Extract Using Open-Source Tool", "Extract Using Enterprise Tool"],
            horizontal=True,
        )

        # Generate Markdown Button
        if st.button("Generate Markdown"):
            if not url_input.strip():
                st.warning("🚨 Please enter a valid URL.")
                return

            with st.spinner("⏳ Processing the website..."):
                method_val = "open-source" if "Open-Source" in extraction_method else "enterprise"
                try:
                    response_data = website_to_markdown(url_input, method_val)
                    md_url = response_data["markdown_url"]
                    st.success("✅ Markdown generated successfully!")
                    st.write(f"📂 Markdown file uploaded to S3: [View Markdown]({md_url})")

                    # Preview and download images
                    if "image_urls" in response_data:  # Assuming the API returns a list of image URLs
                        st.subheader("📷 Extracted Images")
                        for idx, img_url in enumerate(response_data["image_urls"], start=1):
                            response = requests.get(img_url)
                            img = Image.open(BytesIO(response.content))
                            st.image(img, caption=f"Image {idx}", use_column_width=True)
                            st.download_button(
                                label=f"Download Image {idx}",
                                data=response.content,
                                file_name=f"image_{idx}.png",
                                mime="image/png",
                            )
                except Exception as e:
                    st.error(f"❌ An error occurred: {e}")


if __name__ == "__main__":
    main()
