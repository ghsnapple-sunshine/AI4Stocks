from buffett.adapter.enum import Enum


class ComparableEnum(Enum):
    def __lt__(self, other):
        """
        if self is less than other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value < other.value
        return False

    def __le__(self, other):
        """
        if self is less than or equals other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value <= other.value
        return False

    def __gt__(self, other):
        """
        if self is greater than other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value > other.value
        return False

    def __ge__(self, other):
        """
        if self is greater than or equals other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value >= other.value
        return False

    def __eq__(self, other):
        """
        if self is greater than or equals other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value == other.value
        return False

    def __ne__(self, other):
        """
        if self is greater than or equals other

        :param other:
        :return:
        """
        if self.__class__ == other.__class__:
            return self.value == other.value
        return True

    def __hash__(self):
        """
        get hash value

        :return:
        """
        return hash(self.value)
