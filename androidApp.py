from kivy.app import App
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.label import Label
from kivy.uix.boxlayout import BoxLayout
import requests

class DataFetchApp(App):
    def build(self):
        layout = BoxLayout(orientation='vertical')

        # Create a text input for the URL
        self.url_input = TextInput(hint_text='Enter API URL', multiline=False)
        self.text_area = TextInput(hint_text='Data will be displayed here', readonly=True, size_hint_y=None, height=500)
        fetch_button = Button(text='Fetch Data', on_press=self.fetch_data)

        layout.add_widget(self.url_input)
        layout.add_widget(fetch_button)
        layout.add_widget(self.text_area)

        return layout

    def fetch_data(self, instance):
        # Get the URL from the text input
        url = self.url_input.text

        if not url:
            self.text_area.text = 'Error: Please enter a URL'
            return

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            self.text_area.text = str(data)
        else:
            self.text_area.text = 'Error: Unable to fetch data'

if __name__ == '__main__':
    DataFetchApp().run()
