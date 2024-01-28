import os
import psutil


def search_items(root_path, target_name):
    for root, dirs, files in os.walk(root_path):
        if target_name in dirs:
            print("Gefunden (Ordner):", os.path.join(root, target_name))
        for file in files:
            if target_name in file:
                print("Gefunden (Datei):", os.path.join(root, file))


def get_available_drives():
    drives = []
    partitions = psutil.disk_partitions(all=True)
    for partition in partitions:
        if "cdrom" not in partition.opts and partition.fstype != "":
            drives.append(partition.mountpoint)
    return drives


def main():
    drives = get_available_drives()

    target_name = input(
        "Geben Sie den Namen des gesuchten Ordners oder der Datei ein: "
    )

    for drive in drives:
        print("Durchsuche", drive)
        search_items(drive, target_name)


if __name__ == "__main__":
    main()
