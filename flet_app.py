import flet as ft


def main(page: ft.Page):
    def find_option(option_name):
        for option in dropdown.options:
            # print("option:",option)
            # print(option_name == option.key)
            # print(f"{option_name} == {option.key}:")
            if option_name == option.key:
                # print(option_name == option.key)
                # print(f"{option_name} == {option.key}:")
                return option
        return None

    def add_clicked(e):
        input_value = option_textbox.value
        # check if input is not empty
        if input_value is not None and input_value != "":
            # check if input is already in dropdown
            if find_option(input_value) is None:
                dropdown.options.append(ft.dropdown.Option(input_value))
                dropdown.value = input_value
                option_textbox.value = ""
                page.dialog.open = False
                page.update()

    def delete_clicked(e):
        option = find_option(dropdown.value)
        if option is not None:
            dropdown.options.remove(option)
            dropdown.value = None
            page.dialog.open = False
            page.update()

    def close_dlg(e):
        page.dialog.open = False
        page.update()

    def textfield_change(e):
        if option_textbox.value == "":
            create_button.disabled = True
        else:
            create_button.disabled = False
        page.update()

    option_textbox = ft.TextField(on_change=textfield_change)
    dropdown = ft.Dropdown()

    create_button = ft.ElevatedButton(text="Create", on_click=add_clicked)
    delete_button = ft.ElevatedButton(text="Delete", on_click=delete_clicked)
    cancel_button = ft.ElevatedButton(text="Cancel", on_click=close_dlg)

    def set_create_use_case_dialog(e):
        page.dialog = create_use_case_dialog
        page.dialog.open = True
        page.update()

    def set_delete_use_case_dialog(e):
        if dropdown.value is not None and dropdown.value != "":
            delete_use_case_dialog.title = ft.Text(
                f"Are you sure to delete all Configuration of Use case: {dropdown.value}"
            )
            page.dialog = delete_use_case_dialog
            page.dialog.open = True
            page.update()

    create_use_case_dialog = ft.AlertDialog(
        title=ft.Text("Enter Use case name"),
        content=ft.Column(
            [
                ft.Container(
                    content=option_textbox, padding=ft.padding.symmetric(horizontal=5)
                ),
                ft.Row(
                    controls=[create_button, cancel_button],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
            ],
            tight=True,
            alignment=ft.MainAxisAlignment.CENTER,
        ),
        open=False,
        on_dismiss=lambda e: print("Modal create_use_case_dialog dismissed!"),
    )

    delete_use_case_dialog = ft.AlertDialog(
        content=ft.Column(
            [
                ft.Row(
                    controls=[delete_button, cancel_button],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
            ],
            tight=True,
            alignment=ft.MainAxisAlignment.CENTER,
        ),
        open=False,
        on_dismiss=lambda e: print("Modal delete_use_case_dialog dismissed!"),
    )

    open_create_use_case_dialog_button = ft.ElevatedButton(
        "Create Use case", on_click=set_create_use_case_dialog
    )

    open_delete_use_case_dialog_button = ft.ElevatedButton(
        "Delete Use case", on_click=set_delete_use_case_dialog
    )

    headline = ft.Text("Use case Config generator")
    page.add(ft.Container(headline, alignment=ft.Alignment(0, 0)))
    page.add(
        ft.Row(
            controls=[
                dropdown,
                open_create_use_case_dialog_button,
                open_delete_use_case_dialog_button,
            ]
        )
    )


if __name__ == "__main__":
    ft.app(target=main)
