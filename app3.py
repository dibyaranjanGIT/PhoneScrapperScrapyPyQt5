import os
import sys
import time
import pandas as pd
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog, QLineEdit,
    QTableWidget, QTableWidgetItem, QProgressBar, QMessageBox, QHeaderView, QSpacerItem, QSizePolicy, QFrame
)
from PyQt5 import QtWidgets, QtCore
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QSize
from PyQt5.QtGui import QIcon, QColor, QPainter, QBrush, QLinearGradient, QPixmap
import ctypes  

from multiprocessing import Process, Event, Queue
import scrapy 
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from phoneScrapper.spiders.phone_scrapper import PhoneScrapperSpider
from scrapy import signals
from pydispatch import dispatcher

if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
    base_path = base_path + '\phoneScrapper'
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

# Paths for images
base_path = os.path.join(base_path, 'images')

# Setting up the relative file paths
browser_icon_path = os.path.join(base_path, 'browser_icon.png')
cocentrix_icon_path = os.path.join(base_path, 'Concentrix.png')
csv_icon_path = os.path.join(base_path, 'csv.png')
excel_icon_path = os.path.join(base_path, 'excel.png')
arrow_icon_path = os.path.join(base_path,'right-arrows.png')
icon_path = os.path.join(base_path, 'scrapper.png')


# Set the AppUserModelID to ensure the taskbar icon appears
ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('company.app.1')

def run_spider(domains, item_queue, spider_closed_event, pause_event):
    settings = get_project_settings()
    process = CrawlerProcess(settings=settings)

    class CustomPhoneScrapperSpider(PhoneScrapperSpider):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            dispatcher.connect(self.item_scraped_callback, signal=signals.item_scraped)
            dispatcher.connect(self.spider_closed_callback, signal=signals.spider_closed)
            self.pause_event = pause_event

        def item_scraped_callback(self, item, response, spider):
            item_queue.put(dict(item))  # Put scraped items into the queue

        def spider_closed_callback(self, spider):
            spider_closed_event.set()  # Set the event to indicate the spider has closed

        def start_requests(self):
            urls = [self.convert_to_url(domain) for domain in self.domains]
            self.logger.info(f"Starting requests for {len(urls)} URLs")
            for url in urls:
                self.logger.info(f"Requesting URL: {url}")
                while self.pause_event.is_set():  # Check pause event
                    self.logger.info(f"Pausing URL request: {url}")
                    time.sleep(1)
                yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_handle, meta={'parent_url': url, 'is_parent': True})

    process.crawl(CustomPhoneScrapperSpider, domains=domains)
    process.start()
    process.stop()

class ScrapingThread(QThread):
    item_scraped = pyqtSignal(dict)
    spider_closed = pyqtSignal()
    url_processed = pyqtSignal(int, int)  # Emit total contacts found and not found for each URL

    def __init__(self, domains, pause_event):
        super().__init__()
        self.domains = domains
        self.pause_event = pause_event
        self.item_queue = Queue()
        self.spider_closed_event = Event()
        self.process = None

    def run(self):
        self.process = Process(target=run_spider, args=(self.domains, self.item_queue, self.spider_closed_event, self.pause_event))
        self.process.start()
        self.monitor_queue()

    def monitor_queue(self):
        while self.process.is_alive() or not self.item_queue.empty():
            if not self.item_queue.empty():
                item = self.item_queue.get()
                self.item_scraped.emit(item)
                total_found = sum(1 for key in ['phone_number_1', 'phone_number_2', 'phone_number_3'] if item.get(key))
                total_not_found = 3 - total_found
                self.url_processed.emit(total_found, total_not_found)
        self.spider_closed.emit()

    def stop(self):
        if self.process and self.process.is_alive():
            self.process.terminate()
        self.process.join()

class GradientWidget(QWidget):
    def __init__(self):
        super().__init__()

    def paintEvent(self, event):
        painter = QPainter(self)
        gradient = QLinearGradient(0, 0, 0, self.height() * 0.1)
        gradient.setColorAt(0, QColor("#6677cf"))
        gradient.setColorAt(1, QColor("#ffffff"))
        painter.setBrush(QBrush(gradient))
        painter.drawRect(self.rect())

class IconAfterTextButton(QPushButton):
        def __init__(self, text, icon_path, stylesheet=None, icon_size=QSize(16, 16), parent=None):
            super().__init__(parent)

            # Set the button's layout to be horizontal
            self.setLayout(QHBoxLayout())

            # Add the text first
            self.label = QLabel(text)
            self.layout().addWidget(self.label)

            # Add a spacer to push the icon to the end
            self.layout().addStretch()

            # Add the icon
            self.icon_label = QLabel()
            self.icon_label.setPixmap(QIcon(icon_path).pixmap(icon_size))
            self.layout().addWidget(self.icon_label)

            # Set button style (padding and other settings)
            if stylesheet:
                self.setStyleSheet(stylesheet)


class ScrapingApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Phone Number Scraper")
        self.setGeometry(100, 100, 1200, 800)
        # self.setWindowIcon(QIcon('phoneScrapper/scrapper1.ico'))
        self.setWindowIcon(QIcon(icon_path))

        self.file_path = ""
        self.scraped_data = []
        self.pause_event = Event()
        self.scraping_thread = None
        self.start_time = None

        self.total_urls_processed = 0
        self.total_contact_found = 0
        self.total_contact_not_found = 0

        self.initUI()

    def initUI(self):
        main_widget = GradientWidget()
        main_layout = QHBoxLayout(main_widget)  # Main layout is horizontal

        # Table layout on the left side
        table_layout = QVBoxLayout()

        self.table = QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels(["Sl.", "🌐 Website", "Phone Number 1️⃣", "🗺️ Country", "Phone Number 2️⃣", "🗺️ Country", "Phone Number 3️⃣", "🗺️ Country"])

        # Set custom stylesheet for header
        self.table.setStyleSheet("""
            QHeaderView::section {
                background-color: #E6E6FA;
                color: black;
                padding: 4px;
                border: 1px solid lightgray;
                font-family: "Core Sans DS 45 Medium";
            }
        """)

        # Hide the vertical header to remove the default row numbers
        self.table.verticalHeader().setVisible(False)

        # Allow manual resizing of columns
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)  # Allow manual resizing
        header.setStretchLastSection(True)  # Stretch last section to fill the available space

        table_layout.addWidget(self.table)

        # Controls layout on the right side
        controls_layout = QVBoxLayout()

        # Define the custom stylesheet for buttons
        button_stylesheet = """
        QPushButton {
            background-color: #ffffff; 
            color: black;
            border: 1px solid black;
            border-radius: 15px; 
            padding: 8px;
            font-family: "Core Sans DS 45 Medium";
            font-size: 15px;
        }
        QPushButton:hover {
            background-color: #D8BFD8;
        }
        QPushButton:pressed {
            background-color: #2846ca;
        }
        """

        save_csv_button_stylesheet = """
        QPushButton {
            background-color: #ffffff; 
            color: green;
            border: 1px solid #69bb6a;
            border-radius: 15px; 
            padding: 6px;
            font-family: "Core Sans DS 45 Medium";
            font-size: 15px;
        }
        QPushButton:hover {
            background-color: #D8BFD8;
        }
        QPushButton:pressed {
            background-color: #2846ca;
        }
        """

        save_excel_button_stylesheet = """
        QPushButton {
            background-color: #ffffff; 
            color: purple;
            border: 1px solid #776dc1;
            border-radius: 15px; 
            padding: 6px;
            font-family: "Core Sans DS 45 Medium";
            font-size: 15px;
        }
        QPushButton:hover {
            background-color: #D8BFD8;
        }
        QPushButton:pressed {
            background-color: #2846ca;
        }
        """
        single_button_stylesheet = """
        QPushButton {
            background-color: #ffffff; 
            color: black;
            border: 1px solid lightgray;
            border-radius: 15px; 
            padding: 8px;
            font-family: "Core Sans DS 45 Medium";
            font-size: 15px;
        }
        QPushButton:hover {
            background-color: #D8BFD8;
        }
        QPushButton:pressed {
            background-color: #2846ca;
        }
        """

        # Top row buttons layout
        top_buttons_layout = QHBoxLayout()

        self.start_button = QPushButton("▶️ Start")
        self.start_button.setFixedSize(155, 50)
        self.start_button.setStyleSheet(button_stylesheet)
        self.start_button.clicked.connect(self.start_scraping)
        top_buttons_layout.addWidget(self.start_button)

        # Add a horizontal spacer to increase space between top buttons and browse button
        horizontal_spacer_top = QSpacerItem(40, 25, QSizePolicy.Expanding, QSizePolicy.Minimum)
        top_buttons_layout.addItem(horizontal_spacer_top)

        self.stop_button = QPushButton("⛔ Stop")
        self.stop_button.setFixedSize(155, 50)
        self.stop_button.setStyleSheet(button_stylesheet)
        self.stop_button.clicked.connect(self.stop_scraping)
        top_buttons_layout.addWidget(self.stop_button)

        # Add a horizontal spacer to increase space between top buttons and browse button
        horizontal_spacer_top1 = QSpacerItem(40, 25, QSizePolicy.Expanding, QSizePolicy.Minimum)
        top_buttons_layout.addItem(horizontal_spacer_top1)

        self.clear_button = QPushButton("❌ Clear")
        self.clear_button.setFixedSize(155, 50)
        self.clear_button.setStyleSheet(button_stylesheet)
        self.clear_button.clicked.connect(self.clear_results)
        top_buttons_layout.addWidget(self.clear_button)

        # Add a spacer to push buttons to the left
        top_buttons_layout.addStretch(1)

        controls_layout.addLayout(top_buttons_layout)

        # Add a vertical spacer to increase space between top buttons and browse button
        vertical_spacer_top = QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Fixed)
        controls_layout.addItem(vertical_spacer_top)

        # Second row buttons layout
        second_buttons_layout = QHBoxLayout()

        second_buttons_layout.addStretch()

        self.pause_button = QPushButton("⏸️ Pause")
        self.pause_button.setFixedSize(155, 50)
        self.pause_button.setStyleSheet(button_stylesheet)
        self.pause_button.clicked.connect(self.pause_scraping)
        second_buttons_layout.addWidget(self.pause_button)

        # Add a horizontal spacer to increase space between top buttons and browse button
        horizontal_spacer_top2 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        second_buttons_layout.addItem(horizontal_spacer_top2)

        self.resume_button = QPushButton("⏯️ Resume")
        self.resume_button.setFixedSize(155, 50)
        self.resume_button.setStyleSheet(button_stylesheet)
        self.resume_button.clicked.connect(self.resume_scraping)
        second_buttons_layout.addWidget(self.resume_button)

        # Add a horizontal spacer to increase space between top buttons and browse button
        horizontal_spacer_top3 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        second_buttons_layout.addItem(horizontal_spacer_top3)

        # self.help_button = QPushButton("❓Help")
        # self.help_button.setFixedSize(155, 50)
        # self.help_button.setStyleSheet(button_stylesheet)
        # self.help_button.clicked.connect(self.show_help)
        # second_buttons_layout.addWidget(self.help_button)


        # Set alignment of the layout to center
        second_buttons_widget = QWidget()
        second_buttons_widget.setLayout(second_buttons_layout)

        # Add a spacer to push buttons to the left

        controls_layout.addWidget(second_buttons_widget, alignment=Qt.AlignCenter)

        # Add a vertical spacer to increase space 
        vertical_spacer = QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed)
        controls_layout.addItem(vertical_spacer) 

        # File selection layout with Total domains label on top
        # file_selection_layout = QVBoxLayout()

        # self.domain_count_label = QLabel("Total domains: 0")
        # file_selection_layout.addWidget(self.domain_count_label)



        main_layout1 = QVBoxLayout()
        main_container_widget = QWidget()
        main_container_widget.setLayout(main_layout1)

        main_container_widget.setStyleSheet("""
            QWidget {
                border: 2px solid #bfd3f0;
                border-radius: 15px;
                padding: 15px;
            }
        """)
    
        file_layout = QHBoxLayout()

        # Create the label container
        label_container = QFrame()
        label_container.setFrameShape(QFrame.StyledPanel)
        label_container.setStyleSheet("""
            QFrame {
                border: 0px solid black;
                border-radius: 10px;
                padding: 10px;
            }
        """)

        # Create a layout for the label container
        container_layout = QHBoxLayout(label_container)

        # Create a layout to hold the button and the text
        button_layout = QVBoxLayout()

        # Browse button
        self.browse_button = QPushButton()
        self.browse_button.setFixedSize(180, 80)
        self.browse_button.setIcon(QIcon(browser_icon_path))
        self.browse_button.setIconSize(QSize(200, 80))
        self.browse_button.setStyleSheet("padding: 0px; border: 0px solid black;")
        self.browse_button.clicked.connect(self.browse_file)
        # container_layout.addWidget(self.browse_button)
        button_layout.addWidget(self.browse_button)

        spacer = QSpacerItem(0, -20, QSizePolicy.Minimum, QSizePolicy.Fixed)
        button_layout.addItem(spacer)

        # Label below the browse button
        browse_button_label = QLabel("Browse")
        browse_button_label.setAlignment(Qt.AlignCenter)
        browse_button_label.setStyleSheet("""
            QLabel {
                font-size: 16px; 
                font-weight: bold; 
                margin-top: -15px; 
            }
        """)
        button_layout.addWidget(browse_button_label)

        spacer = QSpacerItem(0, -20, QSizePolicy.Minimum, QSizePolicy.Fixed)
        button_layout.addItem(spacer)

        # Label for uploading the excel with an image
        upload_button_label = QLabel()
        upload_button_label.setAlignment(Qt.AlignCenter)
        upload_button_label.setStyleSheet("""
            QLabel {
                font-size: 10px; 
                margin-top: -15px; 
            }
        """)

        # Set the text and image using HTML
        upload_button_label.setText(f"""
            Upload File Format  <img src={excel_icon_path} width='20' height='15'>
        """)

        button_layout.addWidget(upload_button_label)

        # Add the button layout (with the button and label) to the container layout
        container_layout.addLayout(button_layout)

        # Vertical line separator between button and label
        vertical_line = QFrame()
        vertical_line.setFrameShape(QFrame.VLine)
        vertical_line.setFrameShadow(QFrame.Sunken)
        vertical_line.setStyleSheet("background-color: #D3D3D3;")
        container_layout.addWidget(vertical_line)

        # QLabel to show the total domain count
        self.domain_count_label = QLabel("Total Domains Uploaded <br>0")
        self.domain_count_label.setFixedSize(300, 50)
        self.domain_count_label.setAlignment(Qt.AlignCenter)
        self.domain_count_label.setStyleSheet("""
            QLabel {
                border: 0px solid black;
                border-radius: 5px;
                font-size: 15px;  
                font-weight: bold; 
                padding: 5px;
            }
        """)
        container_layout.addWidget(self.domain_count_label)

        # Add the container with the button and label to the main layout
        file_layout.addWidget(label_container)

        file_layout.addStretch(1)

        # file_selection_layout.addLayout(file_layout)
        # controls_layout.addLayout(file_layout) #############
        main_layout1.addLayout(file_layout)


        # Labels for counts (another container for other labels)
        label_container2 = QFrame()
        label_container2.setFrameShape(QFrame.StyledPanel)
        label_container2.setStyleSheet("""
            QFrame {
                border: 0px solid black;
                border-radius: 10px;
                padding: 10px;
            }
        """)

        # Create a layout for the second container
        container_layout2 = QVBoxLayout(label_container2)

        horizontal_line = QFrame()
        horizontal_line.setFrameShape(QFrame.HLine)
        horizontal_line.setFrameShadow(QFrame.Sunken)
        horizontal_line.setStyleSheet("background-color: #D3D3D3;") 
        horizontal_line.setFixedHeight(2)
        container_layout2.addWidget(horizontal_line)

        # Add a vertical spacer to increase space between progress bar and export buttons
        vertical_spacer = QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed)
        container_layout2.addItem(vertical_spacer)

        def create_label(text, bg_color):
            label = QLabel(text)
            label.setFixedHeight(30)
            label.setFixedWidth(400)
            label.setStyleSheet(f"""
                    QLabel {{
                        border: None;
                        background-color: {bg_color};
                        padding: 3px;
                        border-radius: 15px;
                    }}
                """)
            return label

        # Create the labels
        self.total_urls_processed_label = create_label('<span style="color: #b79333; font-weight: bold">URLs Processed:</span> <span style="color: #717171;">0</span>', "#faefd3")
        self.total_urls_processed_label.setFixedWidth(450) 
        container_layout2.addWidget(self.total_urls_processed_label)

        spacer = QSpacerItem(0, 7, QSizePolicy.Minimum, QSizePolicy.Fixed)
        container_layout2.addItem(spacer)

        self.total_contact_found_label = create_label('<span style="color: #64b641; font-weight: bold">Contact Numbers Found:</span> <span style="color: #717171;">0</span>', "#e2f0bd")
        self.total_contact_found_label.setFixedWidth(450) 
        container_layout2.addWidget(self.total_contact_found_label)

        spacer = QSpacerItem(0, 7, QSizePolicy.Minimum, QSizePolicy.Fixed)
        container_layout2.addItem(spacer)

        self.total_contact_not_found_label = create_label('<span style="color: #e75961; font-weight: bold">Contact Numbers Not Found:</span> <span style="color: #717171;">0</span>', "#f5bbc7")
        self.total_contact_not_found_label.setFixedWidth(450) 
        container_layout2.addWidget(self.total_contact_not_found_label)

        spacer = QSpacerItem(0, 7, QSizePolicy.Minimum, QSizePolicy.Fixed)
        container_layout2.addItem(spacer)

        self.success_rate_label = create_label('<span style="color: #245dd6; font-weight: bold">Contact Success Rate:</span> <span style="color: #717171;">0%</span>', "#bcdaf6")
        self.success_rate_label.setFixedWidth(450) 
        container_layout2.addWidget(self.success_rate_label)

        container_layout2.setAlignment(Qt.AlignCenter)


        # Add the second container to the main layout
        main_layout1.addWidget(label_container2)

        # controls_layout.addLayout(main_layout1)
        controls_layout.addWidget(main_container_widget)


        # Add a vertical spacer to increase space between progress bar and export buttons
        vertical_spacer = QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Fixed)
        controls_layout.addItem(vertical_spacer) 

        # Progress bar layout
        progress_layout = QVBoxLayout()

        self.progress_bar = QProgressBar()
        progress_layout.addWidget(self.progress_bar)

        self.time_layout = QHBoxLayout()
        progress_layout.addLayout(self.time_layout)

        self.time_label = QLabel("Elapsed Time: 0s")
        self.time_layout.addWidget(self.time_label)

        # Add a fixed spacer to increase space between time labels
        time_fixed_spacer = QSpacerItem(40, 20, QSizePolicy.Minimum, QSizePolicy.Minimum)
        self.time_layout.addItem(time_fixed_spacer)  # This spacer adds fixed space

        self.remaining_label = QLabel("Time remaining: Calculating...")
        self.time_layout.addWidget(self.remaining_label)

        # Add a horizontal spacer to ensure equal spacing from the right
        horizontal_spacer_controls_p = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        progress_layout.addItem(horizontal_spacer_controls_p)

        controls_layout.addLayout(progress_layout)

        # Single Data View layout
        single_data_layout = QHBoxLayout()

        self.single_url_input = QLineEdit()
        self.single_url_input.setPlaceholderText("SINGLE DATA VIEW")
        self.single_url_input.setFixedSize(500, 30)
        self.single_url_input.setStyleSheet("border: none;") 
        single_data_layout.addWidget(self.single_url_input)
        single_data_layout.setAlignment(Qt.AlignLeft)

        vertical_line2 = QFrame()
        vertical_line2.setFrameShape(QFrame.VLine)
        vertical_line2.setFrameShadow(QFrame.Sunken)
        vertical_line2.setFixedHeight(self.single_url_input.height()) 
        vertical_line2.setStyleSheet("background-color: #D3D3D3;") 
        single_data_layout.addWidget(vertical_line2)

        self.single_start_button = QPushButton()
        self.single_start_button.setFixedSize(38, 30)
        self.single_start_button.setStyleSheet(single_button_stylesheet)
        self.single_start_button.setIcon(QIcon(arrow_icon_path))
        self.single_start_button.setIconSize(QSize(55, 35))
        self.single_start_button.clicked.connect(self.start_single_scraping)
        single_data_layout.addWidget(self.single_start_button)

        single_data_container = QWidget()
        single_data_container.setLayout(single_data_layout)
        single_data_container.setFixedSize(570, 50) 
        single_data_container.setStyleSheet("""
            border: 2px solid #bfd3f0;
            border-radius: 25px;
            padding: 5px; 
        """)

        controls_layout.addWidget(single_data_container)

        # Add a vertical spacer to increase space between top buttons and browse button
        vertical_spacer_top = QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed)
        controls_layout.addItem(vertical_spacer_top)

        # Save buttons layout
        save_layout = QHBoxLayout()

        # self.save_csv_button = IconAfterTextButton("Export as CSV", 'phoneScrapper/images/csv.png', icon_size=QSize(80, 20))
        self.save_csv_button = IconAfterTextButton("Export as CSV", csv_icon_path, icon_size=QSize(80, 20))
        self.save_csv_button.setFixedSize(150, 40)
        self.save_csv_button.setStyleSheet(save_csv_button_stylesheet)
        self.save_csv_button.clicked.connect(lambda: self.save_results('csv'))
        save_layout.addWidget(self.save_csv_button)

        # Add a vertical line between the buttons with fixed height
        vertical_line3 = QFrame()
        vertical_line3.setFrameShape(QFrame.VLine)
        vertical_line3.setFrameShadow(QFrame.Sunken)
        vertical_line3.setFixedHeight(self.save_csv_button.height())  # Set fixed height to match button height
        save_layout.addWidget(vertical_line3)

        self.save_excel_button = IconAfterTextButton("Export as Excel", excel_icon_path, icon_size=QSize(40, 20))
        self.save_excel_button.setFixedSize(150, 40)
        self.save_excel_button.setStyleSheet(save_excel_button_stylesheet)
        self.save_excel_button.clicked.connect(lambda: self.save_results('xlsx'))
        save_layout.addWidget(self.save_excel_button)

        # Add a stretch to push buttons to the right
        save_layout.addStretch()

        # Create a container widget for the save_layout to set alignment
        save_container = QWidget()
        save_container.setLayout(save_layout)
        save_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        # Align the save_container to the right
        controls_layout.addWidget(save_container, alignment=Qt.AlignRight)


        # Add a spacer to push everything to the left
        controls_layout.addSpacerItem(QSpacerItem(20, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))

        # Reduce space at the bottom
        controls_layout.addSpacerItem(QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # Add a container for logo and copyright text with background color
        logo_container = QWidget()
        logo_container.setStyleSheet("background-color: #eff1fd;")
        logo_layout = QVBoxLayout(logo_container)

        logo_widget = QWidget()
        logo_layout_inner = QHBoxLayout(logo_widget)

        self.logo_label = QLabel()
        # self.logo_label.setPixmap(QPixmap('phoneScrapper/images/Concentrix.png').scaled(40, 40, Qt.KeepAspectRatio))
        self.logo_label.setPixmap(QPixmap(cocentrix_icon_path).scaled(40, 40, Qt.KeepAspectRatio))
        self.logo_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        logo_layout_inner.addWidget(self.logo_label)

        # Add a fixed-width spacer widget between logo_label and copyright_label
        spacer_widget = QWidget()
        spacer_widget.setFixedWidth(8) 
        logo_layout_inner.addWidget(spacer_widget)

        self.copyright_label = QLabel("@ Copyright Product by CR Consultancy Service PVT LTD.")
        self.copyright_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.copyright_label.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.copyright_label.setStyleSheet("""
            QLabel {
                font-size: 16px;  
                font-weight: bold; 
            }
        """)
        logo_layout_inner.addWidget(self.copyright_label)

        logo_layout_inner.setContentsMargins(0, 0, 0, 0)
        logo_layout_inner.setSpacing(10)

        logo_widget.setLayout(logo_layout_inner)
        logo_layout.addWidget(logo_widget)

        controls_layout.addWidget(logo_container)
        
        # Create a wrapper widget to contain the control layout with the gradient
        controls_wrapper = QWidget()
        controls_wrapper.setLayout(controls_layout)

        # Add the table layout and controls wrapper to the main layout
        main_layout.addLayout(table_layout)
        main_layout.addWidget(controls_wrapper)

        # Set the stretch factors
        main_layout.setStretch(0, 8)
        main_layout.setStretch(1, 2)
        main_layout.addStretch()

        self.setCentralWidget(main_widget)

        # Timer to update elapsed time and remaining time
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel file", "", "Excel files (*.xlsx)")
        if file_path:
            self.file_path = file_path
            self.domain_count_label.setText(f"Total domains Uploaded <br>{len(pd.read_excel(file_path, header=None)[0])}")
            # self.file_path_label.setText(file_path)

    def start_scraping(self):
        print("start scraping")
        if not self.file_path:
            QMessageBox.warning(self, "Warning", "Please select an Excel file first.")
            return
        if self.scraping_thread and self.scraping_thread.isRunning():
            QMessageBox.warning(self, "Warning", "Scraping is already running.")
            return
        try:
            self.domains = pd.read_excel(self.file_path, header=None)[0].tolist()
            self.domain_count_label.setText(f"Total domains Uploaded <br>{len(self.domains)}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to read Excel file: {e}")
            return

        self.scraped_data.clear()
        self.total_urls_processed = 0
        self.total_contact_found = 0
        self.total_contact_not_found = 0
        self.progress_bar.setValue(0)
        self.table.setRowCount(0)

        self.scraping_thread = ScrapingThread(self.domains, self.pause_event)
        self.scraping_thread.item_scraped.connect(self.item_scraped)
        self.scraping_thread.spider_closed.connect(self.spider_closed)
        self.scraping_thread.url_processed.connect(self.update_counts)  
        self.start_time = time.time() 
        self.scraping_thread.start()
        self.start_button.setEnabled(False)

    def start_single_scraping(self):
        single_url = self.single_url_input.text().strip()
        if not single_url:
            QMessageBox.warning(self, "Warning", "Please enter a single domain.")
            return

        self.scraped_data.clear()
        self.total_urls_processed = 0
        self.total_contact_found = 0
        self.total_contact_not_found = 0
        self.progress_bar.setValue(0)
        self.table.setRowCount(0)

        self.domains = [single_url]  # Set the single domain
        self.scraping_thread = ScrapingThread(self.domains, self.pause_event)
        self.scraping_thread.item_scraped.connect(self.item_scraped)
        self.scraping_thread.spider_closed.connect(self.spider_closed)
        self.scraping_thread.url_processed.connect(self.update_counts) 
        self.start_time = time.time() 
        self.scraping_thread.start()
        self.single_start_button.setEnabled(False)

    def item_scraped(self, item):
        # def format_phone_number(number):
        #     return f"{number}".strip()
        # Check if phone_number_1 and country_1 are not from 'US' or 'CA'
        if item.get('country_1', '') not in ["US", "CA"] and not item.get('phone_number_3', ''):
            item['phone_number_3'] = item.get('phone_number_1', '')
            item['country_3'] = item.get('country_1', '')
            item['phone_number_1'] = ''
            item['country_1'] = ''

        phone_numbers = [
            (item.get('phone_number_1', ''), item.get('country_1', '')),
            (item.get('phone_number_2', ''), item.get('country_2', '')),
            (item.get('phone_number_3', ''), item.get('country_3', ''))
        ]
        
        if item.get('country_1', '') not in ["US", "CA"]:
            item['country_1'] = ''

        if item.get('country_2', '') not in ["US", "CA"]:
            item['country_2'] = ''

        if item.get('country_3', '') not in ["US", "CA"]:
            item['country_3'] = ''

        # Count the number of non-empty phone numbers
        phone_count = sum(1 for number, country in phone_numbers if number)

        # Check if the URL already exists in the table
        existing_row = None
        for row in range(self.table.rowCount()):
            url_item = self.table.item(row, 1)
            if url_item and url_item.text() == item.get('url', ''):
                existing_row = row
                break

        if existing_row is not None:
            # Existing URL found, compare phone numbers
            existing_phone_count = sum(1 for col in [2, 4, 6] if self.table.item(existing_row, col) and self.table.item(existing_row, col).text())
            
            if phone_count > existing_phone_count:
                # Replace the existing row with the new data if it has more phone numbers
                self.update_row(existing_row, item)
            else:
                # Do not add the new item as it has fewer or equal phone numbers
                return
        else:
            # If the URL doesn't exist, add a new row
            self.add_new_row(item)

    def update_row(self, row, item):
        # Update the existing row with the new item data
        phone_numbers = [
            (item.get('phone_number_1', ''), item.get('country_1', '')),
            (item.get('phone_number_2', ''), item.get('country_2', '')),
            (item.get('phone_number_3', ''), item.get('country_3', ''))
        ]

        row_data = [
            str(row + 1),  # Serial number
            item.get('url', '')
        ]

        for number, country in phone_numbers:
            row_data.append(number)
            row_data.append(country)

        # Fill remaining cells with empty strings if there are less than 3 numbers
        while len(row_data) < 8:
            row_data.append('')

        # Update the table with the new row data
        for i, data in enumerate(row_data):
            cell_item = QTableWidgetItem(data)
            if row % 2 == 0:  # Even row
                cell_item.setBackground(QColor("#f0f0f0"))
            else:  # Odd row
                cell_item.setBackground(QColor("#ffffff"))
            self.table.setItem(row, i, cell_item)

    def add_new_row(self, item):
        # Add a new row for the item
        phone_numbers = [
            (item.get('phone_number_1', ''), item.get('country_1', '')),
            (item.get('phone_number_2', ''), item.get('country_2', '')),
            (item.get('phone_number_3', ''), item.get('country_3', ''))
        ]

        row_data = [
            str(self.table.rowCount() + 1),  # Serial number
            item.get('url', '')
        ]

        for number, country in phone_numbers:
            row_data.append(number)
            row_data.append(country)

        # Fill remaining cells with empty strings if there are less than 3 numbers
        while len(row_data) < 8:
            row_data.append('')

        # Insert a new row into the table
        row = self.table.rowCount()
        self.table.insertRow(row)
        for i, data in enumerate(row_data):
            cell_item = QTableWidgetItem(data)
            if row % 2 == 0:  # Even row
                cell_item.setBackground(QColor("#f0f0f0"))
            else:  # Odd row
                cell_item.setBackground(QColor("#ffffff"))
            self.table.setItem(row, i, cell_item)

        self.scraped_data.append(row_data)
        self.table.scrollToItem(self.table.item(self.table.rowCount() - 1, 0))  # Scroll to the new row
            
    def update_counts(self, total_found, total_not_found):
        self.total_urls_processed += 1
        self.total_contact_found += total_found
        self.total_contact_not_found += total_not_found
        success_rate = (self.total_contact_found / (self.total_contact_found + self.total_contact_not_found)) * 100

        self.total_urls_processed_label.setText(f"Total URLs Processed: {self.total_urls_processed}")
        self.total_contact_found_label.setText(f"Contact Numbers Found: {self.total_contact_found}")
        self.total_contact_not_found_label.setText(f"Contact Numbers Not Found: {self.total_contact_not_found}")
        self.success_rate_label.setText(f"Contact Success Rate: {success_rate:.2f}%")
        
        self.update_progress_bar()

    def update_progress_bar(self):
        progress = (self.total_urls_processed / len(self.domains)) * 100 if self.domains else 0
        self.progress_bar.setValue(progress)
        QApplication.processEvents()  

    def spider_closed(self):
        elapsed_time = time.time() - self.start_time
        self.time_label.setText(f"Elapsed Time: {elapsed_time:.2f}s")
        self.remaining_label.setText("Time remaining: 0s")
        self.progress_bar.setValue(100)
        self.timer.stop()
        self.start_button.setEnabled(True)
        self.single_start_button.setEnabled(True) 

    def clear_results(self):
        self.file_path = ""
        self.browse_button.setStyleSheet("")
        self.progress_bar.setValue(0)
        self.table.setRowCount(0)
        self.scraped_data.clear()
        self.total_urls_processed = 0
        self.total_contact_found = 0
        self.total_contact_not_found = 0
        self.total_urls_processed_label.setText("Total URLs Processed: 0")
        self.total_contact_found_label.setText("Contact Numbers Found: 0")
        self.total_contact_not_found_label.setText("Contact Numbers Not Found: 0")
        self.success_rate_label.setText("Contact Success Rate: 0%")
        self.time_label.setText("Elapsed Time: 0s")
        self.remaining_label.setText("Time remaining: Calculating...")
        self.domain_count_label.setText("Total domains Uploaded <br>0")  
        # self.file_path_label.setText("") 
        self.start_button.setEnabled(True)
        self.single_start_button.setEnabled(True)

    def stop_scraping(self):
        if self.scraping_thread:
            self.scraping_thread.stop()

    def pause_scraping(self):
        self.pause_event.set()

    def show_help(self):
        help_text = (
            "<h2>Phone Number Scraper Help</h2>"
            "<p>This application allows you to scrape phone numbers from a list of websites provided in an Excel file.</p>"
            "<h3>How to Use:</h3>"
            "<ol>"
            "<li><b>Select Excel File:</b> Click the browse button to select an Excel file containing the list of domains you want to scrape.</li>"
            "<li><b>Start Scraping:</b> Click the 'Start' button to begin scraping phone numbers from the domains.</li>"
            "<li><b>Pause/Resume Scraping:</b> Use the 'Pause' button to pause the scraping process and the 'Resume' button to continue.</li>"
            "<li><b>Stop Scraping:</b> Click the 'Stop' button to stop the scraping process.</li>"
            "<li><b>Single Data View:</b> Enter a single domain in the input box and click the start button to scrape data from that domain.</li>"
            "<li><b>Export Results:</b> Use the 'Export as CSV' or 'Export as Excel' buttons to save the scraped data.</li>"
            "</ol>"
            "<h3>Additional Features:</h3>"
            "<ul>"
            "<li>The progress bar shows the scraping progress.</li>"
            "<li>The elapsed time and estimated remaining time are displayed during the scraping process.</li>"
            "<li>The table displays the scraped phone numbers and their corresponding countries.</li>"
            "<li>Clear results by clicking the 'Clear' button.</li>"
            "</ul>"
            "<h3>Contact:</h3>"
            "<p>If you have any questions or need further assistance, please contact support@example.com.</p>"
        )
        
        help_dialog = QMessageBox(self)
        help_dialog.setWindowTitle("Help/Documentation")
        help_dialog.setText(help_text)
        help_dialog.setStandardButtons(QMessageBox.Ok)
        help_dialog.exec_()


    def resume_scraping(self):
        self.pause_event.clear()

    def save_results(self, filetype):
        if not self.scraped_data:
            QMessageBox.warning(self, "Warning", "No data to save.")
            return

        if filetype == 'csv':
            file_path, _ = QFileDialog.getSaveFileName(self, "Save as CSV", "", "CSV files (*.csv)")
            if file_path:
                self._save_as_csv(file_path)
        elif filetype == 'xlsx':
            file_path, _ = QFileDialog.getSaveFileName(self, "Save as Excel", "", "Excel files (*.xlsx)")
            if file_path:
                self._save_as_excel(file_path)

    def _save_as_csv(self, file_path):
        try:
            df = pd.DataFrame(self.scraped_data, columns=["Sl", "Website", "Phone Number 1", "Country 1", "Phone Number 2", "Country 2", "Phone Number 3", "Country 3"])
            df.to_csv(file_path, index=False)
            QMessageBox.information(self, "Info", "Data saved successfully as CSV.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save data as CSV: {e}")

    def _save_as_excel(self, file_path):
        try:
            df = pd.DataFrame(self.scraped_data, columns=["Sl", "Website", "Phone Number 1", "Country 1",   "Phone Number 2", "Country 2", "Phone Number 3", "Country 3"])
            df.to_excel(file_path, index=False)
            QMessageBox.information(self, "Info", "Data saved successfully as Excel.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save data as Excel: {e}")

           self.remaining_label.setText("Time remaining: Calculating...")

    def format_time(self, seconds):
        if seconds >= 3600:  # 1 hour = 3600 seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{int(hours)}h {int(minutes)}m"
        elif seconds >= 60:  # 1 minute = 60 seconds
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            return f"{int(minutes)}m {int(remaining_seconds)}s"
        else:
            return f"{int(seconds)}s"

    def update_time(self):
        if self.start_time is not None:
            elapsed_time = time.time() - self.start_time
            formatted_elapsed_time = self.format_time(elapsed_time)
            self.time_label.setText(f"Elapsed Time: {formatted_elapsed_time}")

            # Calculate progress based on total URLs processed
            progress = self.total_urls_processed / len(self.domains) if self.domains else 0

            if progress > 0:
                total_time = elapsed_time / progress
                remaining_time = total_time - elapsed_time
                formatted_remaining_time = self.format_time(remaining_time)
                self.remaining_label.setText(f"Time remaining: {formatted_remaining_time}")
            else:
                self.remaining_label.setText("Time remaining: Calculating...")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    # app.setWindowIcon(QIcon('phoneScrapper/scrapper1.ico')) 
    app.setWindowIcon(QIcon(icon_path)) 
    window = ScrapingApp()
    window.show()
    sys.exit(app.exec_())
