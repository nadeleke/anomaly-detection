__author__ = "Najeem Adeleke, PhD"

class Customer():

    def __init__(self, id):
        self.id = id
        self.friend_ids = []
        self.purchase_ids = []
        #self.network_f_ids = []
        #self.network_p_ids = []

    def add_friend(self, friend_id):
        self.friend_ids.append(friend_id)

    def remove_friend(self, friend_id):
        self.friend_ids.remove(friend_id)

    def update_purchases(self, p_id, T):
        self.purchase_ids.append(p_id)
        if len(self.purchase_ids) > T:
            #print(self.purchase_ids)
            del self.purchase_ids[0]